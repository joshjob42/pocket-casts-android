package au.com.shiftyjelly.pocketcasts.repositories.mimi

import android.content.Context
import au.com.shiftyjelly.pocketcasts.models.entity.BaseEpisode
import au.com.shiftyjelly.pocketcasts.models.entity.PodcastEpisode
import au.com.shiftyjelly.pocketcasts.preferences.Settings
import dagger.hilt.android.qualifiers.ApplicationContext
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.json.JSONObject
import timber.log.Timber

/**
 * Ads and self-promotion that mimi-hub found in an episode's transcript (neural_audio/hub/segments.py), for
 * PlaybackManager to skip. Cached on disk so downloaded episodes skip them offline too.
 *
 * The times are on the timeline of the hub's `.mimi`, so they only apply while that is what plays: a fresh copy
 * of the original enclosure can carry different dynamically inserted ads.
 */
@Singleton
class MimiSegments @Inject constructor(
    @ApplicationContext context: Context,
    private val settings: Settings,
    private val mimiHub: MimiHub,
) {
    data class Segment(val startMs: Long, val endMs: Long, val isAd: Boolean, val label: String)

    private val dir = File(context.filesDir, "mimi_segments")
    private val cache = ConcurrentHashMap<String, List<Segment>>()
    private val lastAttempt = ConcurrentHashMap<String, Long>()
    private val summaryChecked = ConcurrentHashMap<String, Long>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Volatile private var lastUuid: String? = null

    @Volatile private var lastPositionMs = -1L

    @Volatile private var runStartMs = -1L // where playback last jumped to by a seek; -1: since it started on this episode

    /** The segments for [uuid] if known; otherwise null and they're fetched in the background. */
    fun get(uuid: String): List<Segment>? {
        cache[uuid]?.let { return it }
        val file = File(dir, "$uuid.json")
        if (file.exists()) {
            parse(file.readText())?.let { segments ->
                cache[uuid] = segments
                return segments
            }
        }
        fetchAsync(uuid)
        return null
    }

    /**
     * Called about once a second while [uuid] plays. Once its ad marks are here (the hub summarises right after
     * finding them), asks the hub about its summary at most every [SUMMARY_CHECK_MS] until it's fetched or the hub
     * says none is coming ([MimiHub.checkSummary]). Before then it costs nothing: no requests.
     */
    fun onPlaying(uuid: String) {
        if (!mimiHub.isSummariesEnabled) return
        val now = System.currentTimeMillis()
        val previous = summaryChecked[uuid]
        if (previous != null && now - previous < SUMMARY_CHECK_MS) return
        summaryChecked[uuid] = now
        scope.launch {
            if (cache.containsKey(uuid) || File(dir, "$uuid.json").exists()) mimiHub.checkSummary(uuid)
        }
    }

    private fun fetchAsync(uuid: String) {
        if (!mimiHub.isEnabled) return
        val now = System.currentTimeMillis()
        val previous = lastAttempt.put(uuid, now)
        if (previous != null && now - previous < RETRY_MS) {
            lastAttempt[uuid] = previous
            return
        }
        scope.launch {
            // null until the hub has run detection (it does so right after transcribing)
            val text = mimiHub.segmentsJson(uuid) ?: return@launch
            val segments = parse(text) ?: return@launch
            try {
                dir.mkdirs()
                File(dir, "$uuid.json").writeText(text)
            } catch (e: Exception) {
                Timber.w(e, "mimi: could not cache segments of %s", uuid)
            }
            cache[uuid] = segments
            Timber.i("mimi: %d skippable segments for %s", segments.size, uuid)
        }
    }

    private fun parse(text: String): List<Segment>? = try {
        val array = JSONObject(text).getJSONArray("segments")
        (0 until array.length()).map { k ->
            val s = array.getJSONObject(k)
            Segment(
                startMs = (s.getDouble("start") * 1000).toLong(),
                endMs = (s.getDouble("end") * 1000).toLong(),
                isAd = s.optString("kind") != "promo",
                label = s.optString("label"),
            )
        }
    } catch (e: Exception) {
        Timber.w(e, "mimi: bad segments JSON")
        null
    }

    /**
     * What [episode]'s seek bar shows: all its segments, and as cuts the kinds being skipped when "Trim skipped
     * segments" is on. Null unless the hub's `.mimi` is what plays (other files' times don't line up) and segments
     * are known.
     */
    fun timelineFor(episode: BaseEpisode): MimiTimeline? {
        if (!isPlayingMimi(episode)) return null
        val segments = get(episode.uuid)?.takeIf { it.isNotEmpty() } ?: return null
        val cuts = if (settings.mimiTrimTimeline.value) {
            val skipAds = (episode as? PodcastEpisode)?.let { mimiHub.skipsAds(it) } ?: settings.mimiSkipAds.value
            val skipPromos = settings.mimiSkipPromos.value
            segments.filter { if (it.isAd) skipAds else skipPromos }
        } else {
            emptyList()
        }
        return MimiTimeline(segments, cuts)
    }

    /**
     * Segments to fold in [uuid]'s mimi-hub transcript: the kinds being skipped, when "Trim skipped segments" is on.
     * Hub transcripts are on the `.mimi` timeline, so this doesn't depend on what is playing.
     */
    fun transcriptCuts(uuid: String, podcastUuid: String? = null): List<Segment> {
        if (!settings.mimiTrimTimeline.value) return emptyList()
        val skipAds = podcastUuid?.takeIf { it.isNotEmpty() }?.let { mimiHub.skipsAdsPodcast(it) } ?: settings.mimiSkipAds.value
        val skipPromos = settings.mimiSkipPromos.value
        return get(uuid).orEmpty().filter { if (it.isAd) skipAds else skipPromos }
    }

    /** The hub's audio is playing (its .mimi, or the original it kept: the segments' timeline) and the podcast
     * wants the server's ad marks. */
    private fun isPlayingMimi(episode: BaseEpisode): Boolean {
        if (episode is PodcastEpisode && !mimiHub.transcribes(episode)) return false
        return if (episode.isDownloaded) {
            episode.downloadedFilePath?.endsWith(".mimi") == true || mimiHub.isHubSource(episode.uuid)
        } else {
            mimiHub.isStreamingHub(episode.uuid)
        }
    }

    /**
     * Called with the playback position about once a second. Returns the segment to jump past when playback is in
     * one it ran into: one it started in (a pre-roll, or resuming mid-ad) counts, even if its marks only arrive once
     * it's playing. Seeking into a segment doesn't trigger a skip, so seeking back into an ad plays it ([onSeek], and
     * jumps in the position for seeks it wasn't told of).
     */
    fun toSkip(episode: BaseEpisode, positionMs: Long): Segment? {
        if (episode.uuid != lastUuid) {
            runStartMs = -1L
        } else if (lastPositionMs - positionMs > MAX_STEP_BACK_MS || positionMs - lastPositionMs > MAX_TICK_MS) {
            runStartMs = positionMs // a seek
        }
        lastUuid = episode.uuid
        lastPositionMs = positionMs
        val skipAds = (episode as? PodcastEpisode)?.let { mimiHub.skipsAds(it) } ?: settings.mimiSkipAds.value
        val skipPromos = settings.mimiSkipPromos.value
        if (!skipAds && !skipPromos) return null
        if (!isPlayingMimi(episode)) return null
        val segment = get(episode.uuid)?.firstOrNull { s ->
            (if (s.isAd) skipAds else skipPromos) &&
                s.startMs > runStartMs &&
                s.startMs <= positionMs &&
                positionMs < s.endMs - MIN_LEFT_MS
        } ?: return null
        lastPositionMs = segment.endMs
        return segment
    }

    /** A seek to [positionMs] of [episodeUuid]: told apart from resuming there, which the position alone can't do when
     * it's the first thing to happen to the episode (a seek while paused, right after the app started). */
    fun onSeek(episodeUuid: String, positionMs: Long) {
        lastUuid = episodeUuid
        lastPositionMs = positionMs
        runStartMs = positionMs
    }

    private companion object {
        const val RETRY_MS = 15_000L // the hub has them ~15 s after the transcript text; one tiny request
        const val SUMMARY_CHECK_MS = 30_000L // the summary follows the ad marks by a few minutes
        const val MAX_TICK_MS = 6_000L // 1 s ticks at up to 5x speed
        const val MAX_STEP_BACK_MS = 1_000L // less isn't a seek (those come by onSeek)
        const val MIN_LEFT_MS = 1_000L
    }
}
