"""
Record an annotated demo GIF of the 1CijferHO Streamlit app.

Approach: async Playwright with asyncio.gather for periodic screenshots.

Usage (app must be running on port 8502):
    uv run python dev/record_demo.py

Output: src/assets/demo.gif  and  docs/assets/demo.gif
"""

import asyncio
import subprocess
import tempfile
import shutil
from pathlib import Path
from playwright.async_api import async_playwright

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
APP_URL = "http://localhost:8502"
SIZE = {"width": 1280, "height": 800}
ACCENT = "#667eea"
GREEN = "#22c55e"
AMBER = "#f59e0b"
FPS = 8

OUT_GIF_SRC = Path("src/assets/demo.gif")
OUT_GIF_DOCS = Path("docs/assets/demo.gif")


# ---------------------------------------------------------------------------
# Screenshot recorder (async)
# ---------------------------------------------------------------------------

class ScreenRecorder:
    def __init__(self, page, out_dir: Path, fps: int = FPS):
        self._page = page
        self._out_dir = out_dir
        self._interval = 1.0 / fps
        self._frame = 0
        self._task: asyncio.Task | None = None

    def start(self):
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self):
        while True:
            path = self._out_dir / f"frame_{self._frame:05d}.png"
            try:
                await self._page.screenshot(path=str(path))
                self._frame += 1
            except Exception:
                pass
            await asyncio.sleep(self._interval)


# ---------------------------------------------------------------------------
# Annotations
# ---------------------------------------------------------------------------

ANN_STYLE = (
    "@keyframes ann-in    { from { opacity:0; transform:scale(.85) } to { opacity:1; transform:scale(1) } }"
    "@keyframes ann-pulse { 0%,100% { box-shadow:0 0 0 0 var(--ann-color) } 50% { box-shadow:0 0 0 8px transparent } }"
    ".__ann-ring  { animation: ann-in .25s ease, ann-pulse 1.4s .25s ease infinite }"
    ".__ann-badge { animation: ann-in .25s ease }"
)


async def _inject_style(page):
    await page.evaluate("""(style) => {
        if (!document.querySelector('#__ann-style')) {
            const s = document.createElement('style');
            s.id = '__ann-style';
            s.textContent = style;
            document.head.appendChild(s);
        }
    }""", ANN_STYLE)


async def ann(page, locator_or_selector, label, side="right", color=ACCENT):
    await _inject_style(page)

    if isinstance(locator_or_selector, str):
        loc = page.locator(locator_or_selector).first
    else:
        loc = locator_or_selector

    try:
        box = await loc.bounding_box(timeout=3000)
    except Exception:
        return
    if not box:
        return

    await page.evaluate("""([box, label, side, color]) => {
        document.querySelectorAll('.__ann').forEach(n => n.remove());
        const pad = 10;
        const ring = document.createElement('div');
        ring.className = '__ann __ann-ring';
        ring.style.cssText = `--ann-color:${color}55;position:fixed;pointer-events:none;z-index:2147483647;`
            + `left:${box.x-pad}px;top:${box.y-pad}px;width:${box.width+pad*2}px;height:${box.height+pad*2}px;`
            + `border:3px solid ${color};border-radius:8px;`;
        document.body.appendChild(ring);
        const badge = document.createElement('div');
        badge.className = '__ann __ann-badge';
        badge.textContent = label;
        const bw = Math.max(label.length * 7.5 + 20, 120);
        let bLeft, bTop;
        if (side === 'right') { bLeft = box.x + box.width + 14; bTop = box.y + box.height/2 - 13; }
        if (side === 'left')  { bLeft = box.x - bw - 14;        bTop = box.y + box.height/2 - 13; }
        if (side === 'above') { bLeft = box.x;                   bTop = box.y - 36; }
        if (side === 'below') { bLeft = box.x;                   bTop = box.y + box.height + 8; }
        badge.style.cssText = `position:fixed;pointer-events:none;z-index:2147483647;`
            + `left:${bLeft}px;top:${bTop}px;background:${color};color:#fff;`
            + `padding:3px 10px;font:bold 12px/22px monospace;border-radius:3px;`
            + `white-space:nowrap;box-shadow:0 2px 10px rgba(0,0,0,.5);`;
        document.body.appendChild(badge);
    }""", [box, label, side, color])


async def clear_ann(page):
    await page.evaluate("() => document.querySelectorAll('.__ann').forEach(n => n.remove())")


async def pause(page, ms):
    await page.wait_for_timeout(ms)


# ---------------------------------------------------------------------------
# Demo scenes
# ---------------------------------------------------------------------------

async def wait_streamlit(page):
    await page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=20_000)
    await pause(page, 1200)


async def scene_home(page):
    await page.goto(APP_URL)
    await wait_streamlit(page)

    await ann(page, ".hero-banner", "1CijferHO — DUO-data naar CSV/Parquet", side="below", color=ACCENT)
    await pause(page, 2500)
    await clear_ann(page)
    await pause(page, 300)

    await ann(page, ".how-card", "3 stappen: extraheer, valideer, converteer", side="below", color="#7c3aed")
    await pause(page, 2000)
    await clear_ann(page)
    await pause(page, 300)

    demo_btn = page.get_by_role("button", name="Probeer met demo")
    await ann(page, demo_btn, "Start met voorbeelddata", side="left", color=GREEN)
    await pause(page, 2000)
    await clear_ann(page)

    await demo_btn.click()
    await pause(page, 1500)


async def scene_upload(page):
    await wait_streamlit(page)

    alert = page.locator('[data-testid="stAlert"]').first
    try:
        await alert.wait_for(state="visible", timeout=3000)
        await ann(page, alert, "Demo modus — bestanden al aanwezig", side="below", color=GREEN)
        await pause(page, 2200)
        await clear_ann(page)
        await pause(page, 300)
    except Exception:
        pass

    await page.get_by_text("Stap 1 · Metadata extraheren").click()
    await pause(page, 1200)


async def scene_extract(page):
    await wait_streamlit(page)

    title = page.locator("h1").first
    await ann(page, title, "Stap 1 · Veldposities uit .txt bestanden extraheren", side="below", color=ACCENT)
    await pause(page, 2500)
    await clear_ann(page)
    await pause(page, 300)

    # Show the "already done" success state or run button
    run_btn = page.get_by_role("button", name="Extraheren starten").or_(
        page.get_by_role("button", name="Ga door naar stap 2 →")
    )
    try:
        await run_btn.wait_for(state="visible", timeout=2000)
        await ann(page, run_btn, "Leest bestandsbeschrijvingen → JSON/Excel", side="right", color=GREEN)
        await pause(page, 2000)
        await clear_ann(page)
    except Exception:
        pass

    # Highlight any success indicator
    success = page.locator('[data-testid="stSuccess"]').first
    try:
        await success.wait_for(state="visible", timeout=2000)
        await ann(page, success, "Extractie succesvol afgerond", side="right", color=GREEN)
        await pause(page, 2000)
        await clear_ann(page)
    except Exception:
        pass

    await page.get_by_text("Stap 2 · Metadata valideren").click()
    await pause(page, 1200)


async def scene_validate(page):
    await wait_streamlit(page)

    title = page.locator("h1").first
    await ann(page, title, "Stap 2 · Controleer structuur en koppelingen", side="below", color=ACCENT)
    await pause(page, 2500)
    await clear_ann(page)
    await pause(page, 300)

    # Show run button
    validate_btn = page.get_by_role("button", name="Validatie starten")
    try:
        await validate_btn.wait_for(state="visible", timeout=2000)
        await ann(page, validate_btn, "Koppelt ASC-bestanden aan beschrijvingen", side="right", color=GREEN)
        await pause(page, 2000)
        await clear_ann(page)
        await validate_btn.click()
        await page.wait_for_timeout(5000)
    except Exception:
        pass

    await page.get_by_text("Stap 3 · Turbo Conversie").click()
    await pause(page, 1200)


async def scene_convert(page):
    await wait_streamlit(page)

    title = page.locator("h1").first
    await ann(page, title, "Stap 3 · Multiprocessing naar CSV/Parquet", side="below", color=ACCENT)
    await pause(page, 2500)
    await clear_ann(page)
    await pause(page, 300)

    # Preset selector
    preset = page.locator('[data-testid="stSelectbox"]').first
    try:
        await preset.wait_for(state="visible", timeout=2000)
        await ann(page, preset, "Kies een preset voor uitvoerformaat", side="right", color=AMBER)
        await pause(page, 2000)
        await clear_ann(page)
        await pause(page, 300)
    except Exception:
        pass

    # Convert button
    convert_btn = page.get_by_role("button", name="⚡ Start Turbo Convert ⚡")
    try:
        await convert_btn.wait_for(state="visible", timeout=2000)
        await ann(page, convert_btn, "Converteert alle bestanden parallel", side="above", color=GREEN)
        await pause(page, 2500)
        await clear_ann(page)
    except Exception:
        pass

    await page.get_by_text("Stap 4 · Output valideren").click()
    await pause(page, 1200)


async def scene_output(page):
    await wait_streamlit(page)

    title = page.locator("h1").first
    await ann(page, title, "Stap 4 · Output controleren", side="below", color=ACCENT)
    await pause(page, 2000)
    await clear_ann(page)
    await pause(page, 300)

    # Highlight validate button
    val_btn = page.get_by_role("button").first
    try:
        await val_btn.wait_for(state="visible", timeout=2000)
        await ann(page, val_btn, "CSV- en Parquet-bestanden klaar voor analyse", side="right", color=GREEN)
        await pause(page, 2500)
        await clear_ann(page)
    except Exception:
        pass

    await pause(page, 600)


# ---------------------------------------------------------------------------
# GIF assembly
# ---------------------------------------------------------------------------

def frames_to_gif(frames_dir: Path, gif_path: Path):
    gif_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-framerate", str(FPS),
            "-pattern_type", "glob",
            "-i", str(frames_dir / "frame_*.png"),
            "-vf", ",".join([
                "scale=960:-1:flags=lanczos",
                "split[s0][s1]",
                "[s0]palettegen=max_colors=128:stats_mode=diff[p]",
                "[s1][p]paletteuse=dither=bayer:bayer_scale=5",
            ]),
            str(gif_path),
        ],
        check=True,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(frames_dir: Path):
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(viewport=SIZE)

        recorder = ScreenRecorder(page, frames_dir, fps=FPS)
        recorder.start()

        print("Recording demo scenes…")
        await scene_home(page)
        await scene_upload(page)
        await scene_extract(page)
        await scene_validate(page)
        await scene_convert(page)
        await scene_output(page)

        await recorder.stop()
        await browser.close()


def main():
    frames_dir = Path(tempfile.mkdtemp(prefix="1cijferho-frames-"))
    try:
        asyncio.run(run(frames_dir))

        frame_count = len(list(frames_dir.glob("frame_*.png")))
        print(f"Captured {frame_count} frames")

        print(f"Assembling GIF → {OUT_GIF_SRC} …")
        frames_to_gif(frames_dir, OUT_GIF_SRC)
        shutil.copy(OUT_GIF_SRC, OUT_GIF_DOCS)

        size_kb = OUT_GIF_SRC.stat().st_size // 1024
        print(f"Done → {OUT_GIF_SRC}  ({size_kb} KB)")
    finally:
        shutil.rmtree(frames_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
