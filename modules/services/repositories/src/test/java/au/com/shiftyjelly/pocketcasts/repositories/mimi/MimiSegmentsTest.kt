package au.com.shiftyjelly.pocketcasts.repositories.mimi

import au.com.shiftyjelly.pocketcasts.models.entity.PodcastEpisode
import au.com.shiftyjelly.pocketcasts.models.type.EpisodeDownloadStatus
import au.com.shiftyjelly.pocketcasts.preferences.Settings
import au.com.shiftyjelly.pocketcasts.preferences.UserSetting
import java.io.File
import java.util.Date
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(manifest = Config.NONE)
class MimiSegmentsTest {
    private val settings = mock<Settings>()
    private val mimiHub = mock<MimiHub>()
    private lateinit var segments: MimiSegments

    private val episode = PodcastEpisode(
        uuid = "e1",
        publishedDate = Date(),
        downloadStatus = EpisodeDownloadStatus.Downloaded,
        downloadedFilePath = "/somewhere/e1.mimi",
    )

    @Before
    fun setUp() {
        val context = RuntimeEnvironment.getApplication()
        File(context.filesDir, "mimi_segments").apply { mkdirs() }.resolve("e1.json").writeText(
            """{"model": "test", "segments": [
                {"start": 1.1, "end": 61.4, "kind": "ad", "label": "pre-roll"},
                {"start": 600.0, "end": 700.0, "kind": "ad", "label": "mid-roll"},
                {"start": 1000.0, "end": 1012.0, "kind": "promo", "label": "membership"}]}""",
        )
        whenever(mimiHub.transcribes(any())).thenReturn(true)
        whenever(mimiHub.skipsAds(any())).thenReturn(true)
        whenever(mimiHub.skipsAdsPodcast(any())).thenReturn(true)
        whenever(settings.mimiSkipAds).thenReturn(UserSetting.Mock(true, mock()))
        whenever(settings.mimiSkipPromos).thenReturn(UserSetting.Mock(false, mock()))
        segments = MimiSegments(context, settings, mimiHub)
    }

    private fun play(vararg positionsMs: Long) = positionsMs.map { segments.toSkip(episode, it)?.label }

    @Test
    fun `skips a pre-roll when playback starts`() {
        assertEquals(listOf(null, "pre-roll"), play(400, 1_400))
    }

    @Test
    fun `skips a pre-roll at 0_00 whose marks arrive once playback has started`() {
        val dir = File(RuntimeEnvironment.getApplication().filesDir, "mimi_segments")
        val e2 = episode.copy(uuid = "e2", downloadedFilePath = "/somewhere/e2.mimi")
        assertNull(segments.toSkip(e2, 300)) // no marks yet
        dir.resolve("e2.json").writeText("""{"segments": [{"start": 0.0, "end": 45.0, "kind": "ad", "label": "pre-roll"}]}""")
        assertEquals("pre-roll", segments.toSkip(e2, 1_300)?.label)
    }

    @Test
    fun `skips an ad that playback resumes in`() {
        assertEquals(listOf("pre-roll"), play(30_000))
    }

    @Test
    fun `plays an ad seeked into before playback started`() {
        segments.onSeek("e1", 610_000)
        assertEquals(listOf(null, null), play(610_300, 611_300))
    }

    @Test
    fun `plays an ad a short seek landed in`() {
        play(598_000, 599_000)
        segments.onSeek("e1", 601_000)
        assertEquals(listOf(null, null), play(601_300, 602_300))
    }

    @Test
    fun `a small step back in the position isn't a seek`() {
        val dir = File(RuntimeEnvironment.getApplication().filesDir, "mimi_segments")
        val e3 = episode.copy(uuid = "e3", downloadedFilePath = "/somewhere/e3.mimi")
        assertNull(segments.toSkip(e3, 1_300))
        assertNull(segments.toSkip(e3, 1_250))
        dir.resolve("e3.json").writeText("""{"segments": [{"start": 0.0, "end": 45.0, "kind": "ad", "label": "pre-roll"}]}""")
        assertEquals("pre-roll", segments.toSkip(e3, 2_250)?.label)
    }

    @Test
    fun `skips a segment right after a skipped one`() {
        val dir = File(RuntimeEnvironment.getApplication().filesDir, "mimi_segments")
        val e4 = episode.copy(uuid = "e4", downloadedFilePath = "/somewhere/e4.mimi")
        dir.resolve("e4.json").writeText(
            """{"segments": [{"start": 600.0, "end": 660.0, "kind": "ad", "label": "first"},
                {"start": 660.0, "end": 700.0, "kind": "ad", "label": "second"}]}""",
        )
        assertNull(segments.toSkip(e4, 599_500))
        assertEquals("first", segments.toSkip(e4, 600_200)?.label)
        assertEquals("second", segments.toSkip(e4, 660_300)?.label)
    }

    @Test
    fun `slow to land, a skip doesn't fire again`() {
        assertEquals(listOf(null, "mid-roll", null, null, null), play(599_500, 600_200, 601_200, 602_200, 700_100))
    }

    @Test
    fun `skips an ad that playback runs into`() {
        assertEquals(listOf(null, null, "mid-roll", null), play(598_000, 599_000, 600_200, 700_100))
    }

    @Test
    fun `plays an ad that was seeked into`() {
        assertEquals(listOf(null, null, null), play(300_000, 610_000, 611_000))
    }

    @Test
    fun `skips promos only when that setting is on`() {
        assertNull(play(999_500, 1_000_500).last())
        whenever(settings.mimiSkipPromos).thenReturn(UserSetting.Mock(true, mock()))
        assertEquals("membership", play(999_500, 1_000_500).last())
    }

    @Test
    fun `ignores segments when the original file plays`() {
        val original = episode.copy(downloadedFilePath = "/somewhere/e1.mp3")
        segments.toSkip(original, 599_000)
        assertNull(segments.toSkip(original, 600_200))
    }

    @Test
    fun `respects per-podcast ad skip override`() {
        whenever(mimiHub.skipsAds(any())).thenReturn(false)
        assertEquals(listOf(null, null, null), play(598_000, 599_000, 600_200))

        whenever(mimiHub.skipsAds(any())).thenReturn(true)
        assertEquals(listOf(null, null, "mid-roll"), play(598_000, 599_000, 600_200))
    }
}
