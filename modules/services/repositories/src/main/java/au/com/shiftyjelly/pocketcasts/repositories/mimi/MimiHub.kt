package au.com.shiftyjelly.pocketcasts.repositories.mimi

import android.content.Context
import au.com.shiftyjelly.pocketcasts.models.db.dao.TranscriptDao
import au.com.shiftyjelly.pocketcasts.models.entity.BaseEpisode
import au.com.shiftyjelly.pocketcasts.models.entity.PodcastEpisode
import au.com.shiftyjelly.pocketcasts.models.entity.Transcript
import au.com.shiftyjelly.pocketcasts.preferences.Settings
import dagger.hilt.android.qualifiers.ApplicationContext
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import timber.log.Timber
import au.com.shiftyjelly.pocketcasts.localization.R as LR

/**
 * Client for mimi-hub on the server (neural_audio/hub/mimi_hub.py), which converts episodes to `.mimi`
 * (Mimi neural codec, ~2 MB/hour) plus a WebVTT transcript, keyed by the Pocket Casts episode UUID.
 *
 * - Downloads: [substituteForDownload] asks the hub for the episode, requests a conversion if needed and
 *   waits for it (bounded), then points the download at the `.mimi`. Any failure keeps the original.
 * - Streaming: [streamOverride] uses the `.mimi` when the hub already has it, else asks for a conversion.
 * - Transcripts: episodes known to be ready get the hub's VTT as an extra transcript row.
 * - Tracking-prefix redirects are stripped from original URLs when that setting is on ([cleanUrl]).
 *
 * The hub parts are disabled when the hub URL setting is blank.
 */
@Singleton
class MimiHub @Inject constructor(
    @ApplicationContext context: Context,
    private val settings: Settings,
    private val transcriptDao: TranscriptDao,
) {
    private val prefs = context.getSharedPreferences("mimi_hub", Context.MODE_PRIVATE)
    private val podcastPrefs = context.getSharedPreferences("mimi_podcasts", Context.MODE_PRIVATE)
    private val client = OkHttpClient.Builder()
        .connectTimeout(3, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .build()

    private val base: String get() = settings.mimiHubUrl.value.trim().trimEnd('/')

    val isEnabled: Boolean get() = base.isNotEmpty()
    val isSummariesEnabled: Boolean get() = isEnabled && settings.mimiShowSummaries.value

    fun mimiUrl(uuid: String) = "$base/v1/episodes/$uuid.mimi"

    fun vttUrl(uuid: String) = "$base/v1/episodes/$uuid.vtt"

    fun isKnownReady(uuid: String): Boolean {
        forOtherServer()
        return prefs.getBoolean(uuid, false)
    }

    /** The ready flags are the current server's: after a change of server URL (another server's library, e.g.
     * halo to Modal) they start over, so its episodes are requested from the new one. Not the "src:" flags: those
     * describe downloads already on the phone. */
    private fun forOtherServer() {
        if (prefs.getString(SERVER_KEY, null) == base) return
        val stale = prefs.all.keys.filter { !it.startsWith("src:") }
        prefs.edit().apply { stale.forEach { remove(it) } }.putString(SERVER_KEY, base).apply()
    }

    // Per-podcast choices (podcast settings), each Default / On / Off; Default follows the global setting.
    // Compression: play the hub's .mimi. Transcripts & ad marks: the hub's VTT and ad / self-promo segments.
    private fun choice(key: String): MimiConversion = podcastPrefs.getString(key, null)
        ?.let { name -> MimiConversion.entries.firstOrNull { it.name == name } }
        ?: MimiConversion.Default

    private fun setChoice(key: String, value: MimiConversion) {
        podcastPrefs.edit().apply {
            if (value == MimiConversion.Default) remove(key) else putString(key, value.name)
        }.apply()
    }

    private fun MimiConversion.resolve(default: Boolean) = when (this) {
        MimiConversion.On -> true
        MimiConversion.Off -> false
        MimiConversion.Default -> default
    }

    fun compressionFor(podcastUuid: String) = choice(podcastUuid)

    fun setCompression(podcastUuid: String, value: MimiConversion) = setChoice(podcastUuid, value)

    fun transcriptionFor(podcastUuid: String) = choice("$podcastUuid:transcripts")

    fun setTranscription(podcastUuid: String, value: MimiConversion) = setChoice("$podcastUuid:transcripts", value)

    fun compressesPodcast(podcastUuid: String): Boolean = isEnabled && compressionFor(podcastUuid).resolve(settings.mimiConvertByDefault.value)

    fun transcribesPodcast(podcastUuid: String): Boolean = isEnabled && transcriptionFor(podcastUuid).resolve(settings.mimiTranscribeByDefault.value)

    fun compresses(episode: PodcastEpisode) = compressesPodcast(episode.podcastUuid)

    fun transcribes(episode: PodcastEpisode) = transcribesPodcast(episode.podcastUuid)

    fun skipAdsFor(podcastUuid: String) = choice("$podcastUuid:skip_ads")

    fun setSkipAds(podcastUuid: String, value: MimiConversion) = setChoice("$podcastUuid:skip_ads", value)

    fun skipsAdsPodcast(podcastUuid: String): Boolean = isEnabled && skipAdsFor(podcastUuid).resolve(settings.mimiSkipAds.value)

    fun skipsAds(episode: PodcastEpisode) = skipsAdsPodcast(episode.podcastUuid)

    /** The hub handles [episode] at all. Transcripts without compression: the hub keeps the original it processed
     * and serves that ([srcUrl]), since a fresh copy from the publisher can carry other (dynamic) ads. */
    fun usesHub(episode: PodcastEpisode) = compresses(episode) || transcribes(episode)

    fun srcUrl(uuid: String) = "$base/v1/episodes/$uuid.src"

    /** This podcast wants the .mimi and the hub makes them (a hub run with MIMI_HUB_COMPRESS=0 doesn't). */
    private fun hubCompresses(episode: PodcastEpisode, status: JSONObject?) = compresses(episode) && status?.optBoolean("compressed", true) != false

    /** The hub can serve the original it processed: kept, or (MIMI_HUB_SOURCES=fetch) re-fetched from the publisher
     * and still the same file. 409 when the publisher's file changed, e.g. different dynamically inserted ads. */
    private fun sourceServable(uuid: String): Boolean = try {
        val request = Request.Builder().url(srcUrl(uuid)).header("Range", "bytes=0-0").build()
        client.newCall(request).execute().use { r -> r.isSuccessful.also { if (!it) Timber.i("mimi-hub .src: %d", r.code) } }
    } catch (e: Exception) {
        Timber.i("mimi-hub .src unreachable: %s", e.message)
        false
    }

    /** Downloads made from the hub's kept original ([srcUrl]); their timeline is the hub's, like a .mimi's. */
    fun markHubSource(uuid: String) = prefs.edit().putBoolean("src:$uuid", true).apply()

    fun isHubSource(uuid: String) = prefs.getBoolean("src:$uuid", false)

    /** The hub's record for [uuid]; state "missing" if the hub doesn't know it, null if unreachable. */
    fun status(uuid: String): JSONObject? = call(Request.Builder().url("$base/v1/episodes/$uuid").build())?.also {
        if (it.optString("state") == "missing") forgetReady(uuid) // removed there (MIMI_HUB_DATA_MAX_GB)
    }

    /** The hub's ad / self-promotion segments for [uuid] (JSON); null if none yet or unreachable. */
    fun segmentsJson(uuid: String): String? = try {
        client.newCall(Request.Builder().url("$base/v1/episodes/$uuid.segments.json").build()).execute().use { r ->
            if (r.isSuccessful) r.body.string() else null
        }
    } catch (e: Exception) {
        Timber.i("mimi-hub segments unreachable: %s", e.message)
        null
    }

    private val streamingHub = ConcurrentHashMap.newKeySet<String>()

    /** Whether the last stream of [uuid] came from the hub (its `.mimi` or its kept original). */
    fun isStreamingHub(uuid: String): Boolean = streamingHub.contains(uuid)

    /** [priority] 0 = about to be streamed (jumps the hub's queue), 1 = download. The original is kept
     * (keep_source) when [episode] isn't to be compressed. */
    private fun requestConversion(episode: PodcastEpisode, url: String, priority: Int): JSONObject? {
        val body = JSONObject()
            .put("url", url)
            .put("id", episode.uuid)
            .put("title", episode.title)
            .put("priority", priority)
            .put("keep_source", !compresses(episode))
            .toString()
            .toRequestBody("application/json".toMediaType())
        val request = Request.Builder().url("$base/v1/convert").post(body)
        // the user's own key for the server's ad detection; the server holds it in memory only, for this episode
        settings.mimiOpenRouterKey.value.trim().takeIf { it.isNotEmpty() }?.let { request.header("X-OpenRouter-Key", it) }
        return call(request.build())
    }

    private fun call(request: Request): JSONObject? = try {
        client.newCall(request).execute().use { r ->
            when {
                r.code == 404 -> JSONObject().put("state", "missing")
                r.isSuccessful -> JSONObject(r.body.string())
                else -> null.also { Timber.w("mimi-hub %s -> HTTP %d", request.url, r.code) }
            }
        }
    } catch (e: Exception) {
        Timber.i("mimi-hub unreachable: %s", e.message)
        null
    }

    /**
     * Blocking; for DownloadEpisodeWorker. Returns [episode] pointed at the hub's `.mimi` (with fileType
     * audio/x-mimi so it's saved as `<uuid>.mimi`) as soon as the hub has started encoding it: the hub
     * serves the file progressively, so the download runs alongside the conversion (~1-2 min per hour of
     * audio). Otherwise (hub unreachable, failed, or not started within [maxWaitMs]) the original episode,
     * minus tracking redirects if that setting is on.
     */
    fun substituteForDownload(
        episode: BaseEpisode,
        maxWaitMs: Long = 3 * 60_000L,
        isStopped: () -> Boolean = { false },
    ): BaseEpisode {
        if (episode !is PodcastEpisode) return episode
        val original = episode.downloadUrl?.takeIf { it.isNotBlank() } ?: return episode
        val clean = cleanUrl(original)
        val fallback = if (clean != original) episode.copy(downloadUrl = clean) else episode
        if (!usesHub(episode)) return fallback
        var s = status(episode.uuid) ?: return fallback
        var compress = hubCompresses(episode, s)
        // a user-started download jumps the hub's queue (also bumps a waiting Up Next prefetch); transcripts
        // without compression need the hub to have kept the original
        if (s.optString("state") in setOf("missing", "failed", "queued") || (!compress && !s.optBoolean("keep_source"))) {
            s = requestConversion(episode, original, priority = 0) ?: return fallback
            compress = hubCompresses(episode, s)
        }
        fun started(s: JSONObject) = if (compress) {
            s.optString("state") in setOf("encoding", "transcribing", "ready", "failed")
        } else {
            s.optBoolean("source_ready") || s.optString("state") == "failed"
        }
        val deadline = System.currentTimeMillis() + maxWaitMs
        while (!started(s)) {
            if (System.currentTimeMillis() > deadline || isStopped()) return fallback
            Thread.sleep(2_000)
            s = status(episode.uuid) ?: return fallback
        }
        if (s.optString("state") == "failed") return fallback
        if (transcribes(episode)) attachTranscriptWhenReady(episode.uuid)
        if (!compress) {
            if (!sourceServable(episode.uuid)) return fallback // the publisher's file changed: no hub timings
            Timber.i("mimi-hub: downloading %s's original from the hub (%d bytes)", episode.uuid, s.optLong("source_bytes"))
            return episode.copy(downloadUrl = srcUrl(episode.uuid))
        }
        // EpisodeDownloader rejects podcast files under 150 KB; .mimi is 4.4 kb/s, so < ~4.5 min episodes
        if (s.optLong("mimi_bytes") < 160_000) return fallback
        Timber.i("mimi-hub: downloading %s as .mimi (%d bytes)", episode.uuid, s.optLong("mimi_bytes"))
        return episode.copy(downloadUrl = mimiUrl(episode.uuid), fileType = MIME_TYPE)
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val awaitingTranscript = ConcurrentHashMap.newKeySet<String>()

    @Volatile private var lastWarm = 0L

    /** Wakes a hub that scales to zero (hub/modal_app.py) when the app comes to the foreground, so its models
     * load while an episode is picked. At most every 15 min: each wake keeps a billed GPU up for a minute or two,
     * and switching apps while listening mustn't keep it up. A hub without POST /v1/warm answers 404: no-op. */
    fun warm() {
        val now = System.currentTimeMillis()
        if (!isEnabled || now - lastWarm < 15 * 60_000L) return
        lastWarm = now
        scope.launch { call(Request.Builder().url("$base/v1/warm").post(ByteArray(0).toRequestBody()).build()) }
    }

    /** Polls the hub in the background and attaches its transcript once the episode is fully ready. */
    fun attachTranscriptWhenReady(uuid: String) {
        if (isKnownReady(uuid)) {
            scope.launch { attachTranscript(uuid) }
            return
        }
        if (!awaitingTranscript.add(uuid)) return
        scope.launch {
            try {
                repeat(TRANSCRIPT_POLLS) {
                    when (status(uuid)?.optString("state")) {
                        "ready" -> {
                            markReady(uuid)
                            attachTranscript(uuid)
                            return@launch
                        }

                        "failed", "missing" -> return@launch

                        else -> delay(10_000)
                    }
                }
            } finally {
                awaitingTranscript.remove(uuid)
            }
        }
    }

    private val prefetchRequested = ConcurrentHashMap.newKeySet<String>()
    private val readyRechecked = ConcurrentHashMap.newKeySet<String>()

    /**
     * For queued episodes (MimiQueuePrefetcher): makes sure the hub has, or is making, [episode] at normal
     * priority, and attaches its transcript once ready. Returns true if the conversion is pending / in flight.
     * A failed conversion is re-requested at most once per app session, and an episode known to be ready is checked
     * once per session (a hub with a disk cap may have removed it; no more often, as a request wakes a server on
     * Modal). Blocking network calls; call from Dispatchers.IO.
     */
    suspend fun prefetch(episode: PodcastEpisode): Boolean {
        if (!usesHub(episode) || (isKnownReady(episode.uuid) && !readyRechecked.add(episode.uuid))) return false
        val url = episode.downloadUrl?.takeIf { it.isNotBlank() } ?: return false
        val status = status(episode.uuid) ?: return false
        return when (status.optString("state")) {
            "ready" -> {
                markReady(episode.uuid)
                if (transcribes(episode)) attachTranscript(episode.uuid)
                // ads not found for want of a key (a server without one): again with the user's, once per session
                val noAds = status.optString("segments_state") == "failed"
                if (noAds && settings.mimiOpenRouterKey.value.isNotBlank() && prefetchRequested.add(episode.uuid)) {
                    requestConversion(episode, url, priority = 1)
                }
                false
            }

            "missing" -> {
                prefetchRequested.add(episode.uuid)
                requestConversion(episode, url, priority = 1)
                true
            }

            // retried once per session; if this session's own request failed, leave it
            "failed" -> {
                if (prefetchRequested.add(episode.uuid)) {
                    requestConversion(episode, url, priority = 1)
                    true
                } else {
                    false
                }
            }

            else -> true // queued, starting, downloading, encoding, transcribing
        }
    }

    /** [url] without tracking-prefix redirects, if that setting is on. */
    fun cleanUrl(url: String): String = if (settings.stripTrackingRedirects.value) TrackingRedirects.strip(url) else url

    /** What to stream instead of the episode's own URL (null: use it as is). */
    data class StreamOverride(val url: String, val contentType: String?, val isMimi: Boolean)

    /**
     * For streaming: the hub's `.mimi`, even while the hub is still making it. Asks for a priority conversion
     * if needed and waits (≤ ~10 s) for the hub to start encoding; the hub then serves the file as it is
     * encoded (~36x real time) and serves seeks ahead of the encoder first. Falls back to the original URL
     * (minus tracking redirects, if that setting is on) when the hub is off, unreachable or can't start.
     */
    suspend fun streamOverride(episode: BaseEpisode): StreamOverride? = withContext(Dispatchers.IO) {
        if (episode !is PodcastEpisode) return@withContext null
        val original = episode.downloadUrl?.takeIf { it.isNotBlank() } ?: return@withContext null
        if (usesHub(episode)) {
            var s = status(episode.uuid)
            if (s?.optString("state") in setOf("missing", "failed", "queued") ||
                (!hubCompresses(episode, s) && s?.optBoolean("keep_source") == false)
            ) {
                s = requestConversion(episode, original, priority = 0) // queued: bump a prefetch
            }
            val compress = hubCompresses(episode, s)
            fun ready(s: JSONObject?) = if (compress) s?.optString("state") in STREAMABLE else s?.optBoolean("source_ready") == true
            var waits = 0
            while (s != null && !ready(s) && s.optString("state") != "failed" && waits++ < 20) {
                delay(500)
                s = status(episode.uuid)
            }
            if (ready(s) && (compress || sourceServable(episode.uuid))) {
                if (transcribes(episode)) attachTranscriptWhenReady(episode.uuid)
                streamingHub.add(episode.uuid)
                return@withContext if (compress) {
                    StreamOverride(mimiUrl(episode.uuid), MIME_TYPE, isMimi = true)
                } else {
                    StreamOverride(srcUrl(episode.uuid), s?.optString("source_type")?.takeIf { it.isNotEmpty() }, isMimi = false)
                }
            }
        }
        streamingHub.remove(episode.uuid)
        cleanUrl(original).takeIf { it != original }?.let { StreamOverride(it, null, isMimi = false) }
    }

    enum class SendResult { Queued, Processed, NotUsingHub, Unreachable }

    /**
     * Multi-select "Send to Mimi server": queues [episode] on the hub at normal priority. The hub ignores episodes it
     * already has (queued, in progress or done) and re-queues failed ones. Blocking; call from Dispatchers.IO.
     */
    fun sendToServer(episode: PodcastEpisode): SendResult {
        if (!usesHub(episode)) return SendResult.NotUsingHub
        // (with the user's OpenRouter key set, asked anyway: the hub redoes ad detection that failed for want of one)
        if (isKnownReady(episode.uuid) && settings.mimiOpenRouterKey.value.isBlank()) return SendResult.Processed
        val url = episode.downloadUrl?.takeIf { it.isNotBlank() } ?: return SendResult.NotUsingHub
        val r = requestConversion(episode, url, priority = 1) ?: return SendResult.Unreachable
        if (r.optString("state") != "ready") return SendResult.Queued
        markReady(episode.uuid)
        if (transcribes(episode)) scope.launch { attachTranscript(episode.uuid) }
        return SendResult.Processed
    }

    private fun markReady(uuid: String) {
        forOtherServer()
        prefs.edit().putBoolean(uuid, true).apply()
    }

    private fun forgetReady(uuid: String) {
        prefs.edit().remove(uuid).apply()
    }

    /** The hub transcript row for [uuid], if the hub is known to have it (and [podcastUuid], if given, wants it). */
    fun transcriptFor(uuid: String, podcastUuid: String? = null): Transcript? = if (
        isEnabled && isKnownReady(uuid) && (podcastUuid == null || transcribesPodcast(podcastUuid))
    ) {
        Transcript(episodeUuid = uuid, url = vttUrl(uuid), type = "text/vtt", isGenerated = false)
    } else {
        null
    }

    /** Adds the hub transcript to the episode's stored transcripts (keeping the existing ones). */
    suspend fun attachTranscript(uuid: String) {
        val ours = transcriptFor(uuid) ?: return
        val existing = transcriptDao.observeTranscripts(uuid).first()
        if (existing.none { it.url == ours.url }) transcriptDao.replaceAll(existing + ours)
        if (isSummariesEnabled) fetchSummaryAsync(uuid)
    }

    // ---------- backlogs (the hub's /v1/backlog: many episodes at once, big ones in bulk on Modal) ----------

    /** Which of a podcast's episodes: all (nothing set), the [latest] N, published on or after [after] and before
     * [before] (YYYY-MM-DD). */
    data class BacklogRange(val latest: Int? = null, val after: String? = null, val before: String? = null)

    /** What a backlog would do: [new] of its [episodes] not on the server yet, [hours] of them, converted in bulk on
     * Modal ([onModal]: about [usd] and [minutes]) or in the server's own queue. */
    data class BacklogPlan(val episodes: Int, val new: Int, val hours: Double, val onModal: Boolean, val usd: Double, val minutes: Int?)

    data class BacklogJob(
        val id: String,
        val state: String,
        val total: Int,
        val converted: Int,
        val ads: Int,
        val summaries: Int,
        val onModal: Boolean,
        val error: String?,
    ) {
        val active: Boolean get() = state !in setOf("done", "failed", "cancelled")
    }

    private fun postJson(path: String, body: JSONObject): JSONObject? = call(
        Request.Builder().url("$base$path").post(body.toString().toRequestBody("application/json".toMediaType())).build(),
    )

    private fun JSONObject.toPlan() = if (has("episodes")) {
        BacklogPlan(
            episodes = optInt("episodes"),
            new = optInt("new"),
            hours = optDouble("hours", 0.0),
            onModal = optString("mode") == "modal",
            usd = optDouble("usd", 0.0),
            minutes = if (isNull("minutes")) null else optInt("minutes"),
        )
    } else {
        null
    }

    private fun JSONObject.toJob() = if (has("id")) {
        BacklogJob(
            id = optString("id"),
            state = optString("state"),
            total = optInt("total"),
            converted = optInt("converted"),
            ads = optInt("ads"),
            summaries = optInt("summaries"),
            onModal = optString("mode") == "modal",
            error = optString("error").takeIf { it.isNotBlank() && it != "null" },
        )
    } else {
        null
    }

    private fun backlogBody(podcastUuid: String, range: BacklogRange) = JSONObject().put("podcast_uuid", podcastUuid).apply {
        range.latest?.let { put("latest", it) }
        range.after?.takeIf { it.isNotBlank() }?.let { put("after", it) }
        range.before?.takeIf { it.isNotBlank() }?.let { put("before", it) }
    }

    /** Blocking (Dispatchers.IO): what converting [range] of the podcast would do; null if the server can't say. */
    fun previewBacklog(podcastUuid: String, range: BacklogRange): BacklogPlan? = postJson("/v1/backlog", backlogBody(podcastUuid, range).put("preview", true))?.toPlan()

    /** Blocking: converts [range] of the podcast on the server; the job, or null. */
    fun startBacklog(podcastUuid: String, range: BacklogRange): BacklogJob? = postJson("/v1/backlog", backlogBody(podcastUuid, range))?.toJob()

    /** Blocking: [episodes] (e.g. a multi-select) as one backlog, so many go to the bulk path; the job, or null. */
    fun startBacklog(episodes: List<PodcastEpisode>): BacklogJob? {
        val list = JSONArray()
        episodes.filter { usesHub(it) && !it.downloadUrl.isNullOrBlank() }.forEach {
            list.put(JSONObject().put("id", it.uuid).put("url", it.downloadUrl).put("title", it.title).put("duration_s", it.duration))
        }
        if (list.length() == 0) return null
        return postJson("/v1/backlog", JSONObject().put("episodes", list))?.toJob()
    }

    /** Blocking: the podcast's newest backlog job on this server, if any. */
    fun backlogFor(podcastUuid: String): BacklogJob? = try {
        client.newCall(Request.Builder().url("$base/v1/backlogs?podcast_uuid=$podcastUuid").build()).execute().use { r ->
            if (!r.isSuccessful) return null
            JSONArray(r.body.string()).optJSONObject(0)?.toJob()
        }
    } catch (e: Exception) {
        Timber.i("mimi-hub backlogs: %s", e.message)
        null
    }

    /** Blocking: stops a backlog (what's converted so far stays). */
    fun cancelBacklog(id: String): BacklogJob? = postJson("/v1/backlogs/$id/cancel", JSONObject())?.toJob()

    // ---------- executive summaries (the hub's MIMI_HUB_SUMMARIZER: made after the ad marks) ----------

    private fun summaryUrl(uuid: String) = "$base/v1/episodes/$uuid.summary.json"

    private val summaryDir by lazy { File(context.filesDir, "mimi_summaries").apply { mkdirs() } }
    private val summaryTitle = context.getString(LR.string.mimi_summary_title)
    private val summaryCache = ConcurrentHashMap<String, String>()
    private val summaryArrivedFlow = MutableSharedFlow<String>(extraBufferCapacity = 16)
    private val noSummaryAnswers = ConcurrentHashMap<String, Int>()
    private val pendingAnswers = ConcurrentHashMap<String, Int>()
    private val missingSummaries = ConcurrentHashMap.newKeySet<String>()
    private val summaryRetries = ConcurrentHashMap<String, Int>()
    private val serverFeatures = ConcurrentHashMap<String, Boolean>() // server URL -> it makes summaries

    /** UUIDs of episodes whose summary was just fetched from the hub, so screens showing them can reload. */
    val summaryArrived: SharedFlow<String> = summaryArrivedFlow.asSharedFlow()

    /**
     * Whether this server makes summaries (GET /v1/features), asked once per server and session: no summary request
     * at all to one that doesn't (a hub without that endpoint, or with summaries off). Blocking.
     */
    private fun serverSummarizes(): Boolean {
        serverFeatures[base]?.let { return it }
        val features = call(Request.Builder().url("$base/v1/features").build()) ?: return false // unreachable: ask again later
        return features.optBoolean("summaries", false).also { serverFeatures[base] = it }
    }

    /** [uuid]'s summary for its show notes, if it's on the phone; else fetched in the background ([summaryArrived]). */
    suspend fun getSummary(uuid: String, podcastUuid: String?): String? = withContext(Dispatchers.IO) {
        if (!isSummariesEnabled || (podcastUuid != null && !transcribesPodcast(podcastUuid))) return@withContext null
        cachedSummary(uuid) ?: run {
            fetchSummaryAsync(uuid)
            null
        }
    }

    /** The summary if it's on the phone already; never asks the hub. */
    private fun cachedSummary(uuid: String): String? {
        summaryCache[uuid]?.let { return it }
        val file = File(summaryDir, "$uuid.html")
        if (!file.exists()) return null
        return file.readText().also { summaryCache[uuid] = it }
    }

    /**
     * For an episode that's playing with its ad marks known ([MimiSegments.onPlaying]): fetches the summary once
     * the hub has made it. The hub writes it right after the ad marks and says "pending" meanwhile. An episode it
     * won't summarise is dropped for the session: at once for "failed", after two answers with no summary state (the
     * hub sets "pending" just after saving the ad marks), after MAX_PENDING_CHECKS of "pending" (one stuck there).
     */
    suspend fun checkSummary(uuid: String) {
        withContext(Dispatchers.IO) {
            if (!isSummariesEnabled || (noSummaryAnswers[uuid] ?: 0) >= 2 || cachedSummary(uuid) != null) return@withContext
            if (!serverSummarizes()) return@withContext
            val s = status(uuid) ?: return@withContext // hub unreachable: the next check tries again
            when (s.optString("summary_state")) {
                "ready" -> fetchSummaryDirect(uuid)
                "pending" -> if (pendingAnswers.merge(uuid, 1, Int::plus)!! >= MAX_PENDING_CHECKS) noSummaryAnswers[uuid] = 2
                "failed" -> noSummaryAnswers[uuid] = 2
                else -> noSummaryAnswers.merge(uuid, 1, Int::plus)
            }
        }
    }

    /** In the background, unless the hub already said it has none this session, or makes none. */
    fun fetchSummaryAsync(uuid: String) {
        if (!isSummariesEnabled || uuid in missingSummaries) return
        scope.launch {
            if (serverSummarizes()) fetchSummaryDirect(uuid, rememberMissing = true)
        }
    }

    /** [rememberMissing]: a 404 means "don't ask again this session" (a summary still pending is picked up while the
     * episode plays: [checkSummary]). */
    suspend fun fetchSummaryDirect(uuid: String, rememberMissing: Boolean = false): String? = withContext(Dispatchers.IO) {
        if (!isSummariesEnabled) return@withContext null
        try {
            val req = Request.Builder().url(summaryUrl(uuid)).build()
            client.newCall(req).execute().use { res ->
                if (res.code == 404 && rememberMissing) summaryNotThereYet(uuid)
                if (!res.isSuccessful) return@withContext null
                val summary = sanitizeSummary(JSONObject(res.body.string()).optString("summary", ""))
                if (summary.isNotBlank()) {
                    missingSummaries.remove(uuid)
                    File(summaryDir, "$uuid.html").writeText(summary)
                    summaryCache[uuid] = summary
                    summaryArrivedFlow.tryEmit(uuid)
                    return@withContext summary
                }
            }
        } catch (e: Exception) {
            Timber.w(e, "Failed to fetch summary for $uuid")
        }
        null
    }

    /**
     * A 404 for [uuid]'s summary: if the hub is still converting the episode or making its summary (asked for as it
     * was queued or downloaded, a summary follows its ad marks by a minute or two), looked for again every
     * [SUMMARY_RETRY_MS], [MAX_SUMMARY_RETRIES] times at most ([summaryArrived] then updates the show notes); if it
     * says none is coming, not asked for again this session.
     */
    private fun summaryNotThereYet(uuid: String) {
        val s = status(uuid)
        val coming = s != null && (
            s.optString("state") !in setOf("ready", "failed", "missing") ||
                s.optString("summary_state") == "pending" ||
                (s.optString("state") == "ready" && s.optString("segments_state") == "pending")
            )
        if (!coming) {
            missingSummaries.add(uuid)
            return
        }
        if (summaryRetries.merge(uuid, 1, Int::plus)!! > MAX_SUMMARY_RETRIES) return
        scope.launch {
            delay(SUMMARY_RETRY_MS)
            fetchSummaryDirect(uuid, rememberMissing = true)
        }
    }

    /** Below the episode's show notes: a rule, a title, the summary (no styling of its own: the show notes' theme). */
    fun formatSummaryCard(summaryHtml: String): String = "<hr /><h3>$summaryTitle</h3>$summaryHtml"

    companion object {
        const val MIME_TYPE = "audio/x-mimi"
        private const val SERVER_KEY = "server:url" // whose ready flags these are
        private val HUB_VTT = Regex(".*/v1/episodes/[A-Za-z0-9_-]+\\.vtt$")
        private const val MAX_PENDING_CHECKS = 20 // x 30 s of playing (MimiSegments): 10 min
        private const val SUMMARY_RETRY_MS = 15_000L
        private const val MAX_SUMMARY_RETRIES = 20 // x 15 s: 5 min (a long episode converting, then its ads and summary)
        private val SUMMARY_TAGS = setOf("h4", "p", "ul", "ol", "li", "strong", "em", "b", "i", "br")
        private val TAG = Regex("<(/?)([a-zA-Z][a-zA-Z0-9]*)[^>]*>")

        /** Only the summary's few tags, without attributes (the hub already does this; the show notes are a WebView). */
        fun sanitizeSummary(html: String): String = TAG.replace(html) { m ->
            val tag = m.groupValues[2].lowercase()
            if (tag in SUMMARY_TAGS) "<${m.groupValues[1]}$tag>" else ""
        }

        /**
         * True for mimi-hub transcripts. Their cue times are on the timeline of the hub's `.mimi`, which is
         * what plays, so they can be highlighted / seeked by playback position directly (no fingerprinting).
         */
        fun isHubTranscriptUrl(url: String): Boolean = HUB_VTT.matches(url)
        private const val TRANSCRIPT_POLLS = 120 // x 10 s
        private val STARTING = setOf("queued", "starting", "downloading")
        private val STREAMABLE = setOf("encoding", "transcribing", "ready")
    }
}
