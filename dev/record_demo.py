"""
Record an annotated demo GIF of the 1CijferHO Streamlit app.

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
APP_URL   = "http://localhost:8502"
SIZE      = {"width": 1280, "height": 780}
ACCENT    = "#667eea"
GREEN     = "#22c55e"
AMBER     = "#f59e0b"
FPS       = 12

OUT_GIF_SRC  = Path("src/assets/demo.gif")
OUT_GIF_DOCS = Path("docs/assets/demo.gif")


# ---------------------------------------------------------------------------
# Screenshot recorder
# ---------------------------------------------------------------------------

class ScreenRecorder:
    def __init__(self, page, out_dir: Path, fps: int = FPS):
        self._page     = page
        self._out_dir  = out_dir
        self._interval = 1.0 / fps
        self._frame    = 0
        self._task     = None

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
# Cursor simulation
# ---------------------------------------------------------------------------

CURSOR_STYLE = """
#__fake-cursor {
    position: fixed;
    width: 20px; height: 20px;
    pointer-events: none;
    z-index: 2147483646;
    transition: left 0.25s ease, top 0.25s ease;
}
#__fake-cursor::before {
    content: '';
    position: absolute;
    left: 0; top: 0;
    width: 0; height: 0;
    border-style: solid;
    border-width: 0 6px 18px 6px;
    border-color: transparent transparent #1a1a1a transparent;
    transform: rotate(-30deg) skewY(10deg);
    filter: drop-shadow(1px 1px 1px rgba(255,255,255,.6));
}
"""


async def _ensure_cursor(page):
    await page.evaluate("""(style) => {
        if (document.getElementById('__fake-cursor')) return;
        const s = document.createElement('style');
        s.textContent = style;
        document.head.appendChild(s);
        const c = document.createElement('div');
        c.id = '__fake-cursor';
        c.style.left = '-40px';
        c.style.top  = '-40px';
        document.body.appendChild(c);
    }""", CURSOR_STYLE)


async def move_cursor(page, x: float, y: float):
    await _ensure_cursor(page)
    await page.evaluate("""([x, y]) => {
        const c = document.getElementById('__fake-cursor');
        if (c) { c.style.left = x + 'px'; c.style.top = y + 'px'; }
    }""", [x, y])


async def cursor_to(page, locator):
    """Move fake cursor to the centre of *locator*."""
    try:
        box = await locator.bounding_box(timeout=3000)
    except Exception:
        return
    if box:
        await move_cursor(page, box["x"] + box["width"] / 2 - 10,
                                box["y"] + box["height"] / 2 - 10)


# ---------------------------------------------------------------------------
# Annotations
# ---------------------------------------------------------------------------

ANN_STYLE = (
    "@keyframes ann-in    { from { opacity:0; transform:scale(.85) } to { opacity:1; transform:scale(1) } }"
    "@keyframes ann-pulse { 0%,100% { box-shadow:0 0 0 0 var(--ann-color) } 50% { box-shadow:0 0 0 8px transparent } }"
    ".__ann-ring  { animation: ann-in .2s ease, ann-pulse 1.4s .2s ease infinite }"
    ".__ann-badge { animation: ann-in .2s ease }"
)


async def _inject_ann_style(page):
    await page.evaluate("""(style) => {
        if (!document.querySelector('#__ann-style')) {
            const s = document.createElement('style');
            s.id = '__ann-style';
            s.textContent = style;
            document.head.appendChild(s);
        }
    }""", ANN_STYLE)


async def ann(page, locator, label, side="right", color=ACCENT):
    """Pulsing ring + badge. locator may be a Playwright locator or CSS string."""
    await _inject_ann_style(page)
    if isinstance(locator, str):
        locator = page.locator(locator).first
    try:
        box = await locator.bounding_box(timeout=3000)
    except Exception:
        return
    if not box:
        return

    await page.evaluate("""([box, label, side, color]) => {
        document.querySelectorAll('.__ann').forEach(n => n.remove());
        const pad = 8;
        const ring = document.createElement('div');
        ring.className = '__ann __ann-ring';
        ring.style.cssText =
            `--ann-color:${color}55;position:fixed;pointer-events:none;z-index:2147483647;`
          + `left:${box.x-pad}px;top:${box.y-pad}px;`
          + `width:${box.width+pad*2}px;height:${box.height+pad*2}px;`
          + `border:2.5px solid ${color};border-radius:6px;`;
        document.body.appendChild(ring);

        const badge = document.createElement('div');
        badge.className = '__ann __ann-badge';
        badge.textContent = label;
        const bw = Math.max(label.length * 7.2 + 20, 100);
        let bLeft, bTop;
        if (side === 'right')  { bLeft = box.x + box.width + 12;  bTop = box.y + box.height/2 - 13; }
        if (side === 'left')   { bLeft = box.x - bw - 12;         bTop = box.y + box.height/2 - 13; }
        if (side === 'above')  { bLeft = box.x;                    bTop = box.y - 34; }
        if (side === 'below')  { bLeft = box.x;                    bTop = box.y + box.height + 10; }
        badge.style.cssText =
            `position:fixed;pointer-events:none;z-index:2147483647;`
          + `left:${bLeft}px;top:${bTop}px;background:${color};color:#fff;`
          + `padding:3px 9px;font:bold 11.5px/22px monospace;`
          + `border-radius:3px;white-space:nowrap;box-shadow:0 2px 8px rgba(0,0,0,.45);`;
        document.body.appendChild(badge);
    }""", [box, label, side, color])


async def clear_ann(page):
    await page.evaluate("() => document.querySelectorAll('.__ann').forEach(n => n.remove())")


async def pause(page, ms):
    await page.wait_for_timeout(ms)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def wait_streamlit(page):
    await page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=20_000)
    await pause(page, 900)


async def smooth_scroll(page, distance: int, steps: int = 8):
    step = distance // steps
    for _ in range(steps):
        await page.evaluate(f"() => window.scrollBy(0, {step})")
        await pause(page, 60)


# ---------------------------------------------------------------------------
# Demo scenes
# ---------------------------------------------------------------------------

async def scene_home(page):
    await page.goto(APP_URL)
    await wait_streamlit(page)

    # Hero banner
    hero = page.locator(".hero-banner")
    await ann(page, hero, "DUO vaste-breedte data → leesbare CSV/Parquet", side="below", color=ACCENT)
    await pause(page, 2200)
    await clear_ann(page)
    await pause(page, 200)

    # How-it-works: annoteer de middelste kaart (de workflow)
    cards = page.locator(".how-card")
    await ann(page, cards.nth(1), "3 stappen — geen code nodig", side="below", color="#7c3aed")
    await pause(page, 1800)
    await clear_ann(page)
    await pause(page, 200)

    # Cursor naar demo-knop, klik
    demo_btn = page.get_by_role("button", name="Probeer met demo")
    await cursor_to(page, demo_btn)
    await pause(page, 600)
    await ann(page, demo_btn, "Werkt direct met voorbeelddata", side="left", color=GREEN)
    await pause(page, 1800)
    await clear_ann(page)
    await pause(page, 200)
    await demo_btn.click()
    await pause(page, 1200)


async def scene_upload(page):
    await wait_streamlit(page)

    # Demo-modus badge
    badge = page.locator('[data-testid="stAlert"]').first
    try:
        await badge.wait_for(state="visible", timeout=3000)
        await cursor_to(page, badge)
        await ann(page, badge, "Bestanden geladen — geen upload nodig", side="below", color=GREEN)
        await pause(page, 2000)
        await clear_ann(page)
        await pause(page, 200)
    except Exception:
        pass

    # Bestand-overzicht: klap bestandsbeschrijvingen open
    expanders = page.locator('[data-testid="stExpander"]')
    first_exp = expanders.first
    try:
        await first_exp.wait_for(state="visible", timeout=3000)
        await cursor_to(page, first_exp)
        await ann(page, first_exp, "26 bestanden: beschrijvingen + Dec-tabellen + data", side="above", color=ACCENT)
        await pause(page, 2000)
        await clear_ann(page)
        await first_exp.click()
        await pause(page, 800)
    except Exception:
        pass

    # Cursor naar "Ga door naar stap 1 →"
    fwd = page.get_by_role("button", name="Ga door naar stap 1 →")
    try:
        await fwd.wait_for(state="visible", timeout=3000)
        await cursor_to(page, fwd)
        await pause(page, 500)
        await fwd.click()
    except Exception:
        await page.get_by_text("Stap 1 · Metadata extraheren").click()
    await pause(page, 1200)


async def scene_extract(page):
    await wait_streamlit(page)

    run_btn = page.get_by_role("button", name="Extraheren starten")
    try:
        await run_btn.wait_for(state="visible", timeout=3000)
        await cursor_to(page, run_btn)
        await ann(page, run_btn, "Parseert veldposities uit .txt bestanden → JSON + Excel", side="right", color=GREEN)
        await pause(page, 2200)
        await clear_ann(page)
        await run_btn.click()
        # Wacht op console log
        await pause(page, 7000)
    except Exception:
        pass

    # Laat console log zien als die er is
    console = page.locator('[data-testid="stExpander"]').filter(has_text="Console Log")
    try:
        await console.wait_for(state="visible", timeout=3000)
        await console.click()
        await pause(page, 1200)
    except Exception:
        pass

    # Door naar stap 2
    fwd = page.get_by_role("button", name="Ga door naar stap 2 →")
    try:
        await fwd.wait_for(state="visible", timeout=3000)
        await cursor_to(page, fwd)
        await pause(page, 400)
        await fwd.click()
    except Exception:
        await page.get_by_text("Stap 2 · Metadata valideren").click()
    await pause(page, 1200)


async def scene_validate(page):
    await wait_streamlit(page)

    validate_btn = page.get_by_role("button", name="Validatie starten")
    try:
        await validate_btn.wait_for(state="visible", timeout=3000)
        await cursor_to(page, validate_btn)
        await ann(page, validate_btn, "Koppelt ASC-bestanden aan metadata — controleert structuur", side="right", color=GREEN)
        await pause(page, 2200)
        await clear_ann(page)
        await validate_btn.click()
        await pause(page, 6000)
    except Exception:
        pass

    # Laat console log zien
    console = page.locator('[data-testid="stExpander"]').filter(has_text="Console Log")
    try:
        await console.wait_for(state="visible", timeout=3000)
        await console.click()
        await pause(page, 1000)
    except Exception:
        pass

    fwd = page.get_by_role("button", name="Ga door naar stap 3 →")
    try:
        await fwd.wait_for(state="visible", timeout=3000)
        await cursor_to(page, fwd)
        await pause(page, 400)
        await fwd.click()
    except Exception:
        await page.get_by_text("Stap 3 · Turbo Conversie").click()
    await pause(page, 1200)


async def scene_convert(page):
    await wait_streamlit(page)

    # Status: bestanden klaar
    status = page.locator('[data-testid="stAlert"]').first
    try:
        await status.wait_for(state="visible", timeout=3000)
        await ann(page, status, "16 bestanden klaar voor conversie", side="below", color=GREEN)
        await pause(page, 1800)
        await clear_ann(page)
        await pause(page, 200)
    except Exception:
        pass

    # Scroll naar uitvoervarianten
    await smooth_scroll(page, 350)
    await pause(page, 400)

    # Uitvoervarianten: _decoded en _enriched
    decoded = page.locator("text=_decoded").first
    try:
        await decoded.wait_for(state="visible", timeout=3000)
        await ann(page, decoded, "Codes → leesbare labels via Dec_* tabellen", side="right", color=AMBER)
        await pause(page, 2000)
        await clear_ann(page)
        await pause(page, 200)
    except Exception:
        pass

    enriched = page.locator("text=_enriched").first
    try:
        await enriched.wait_for(state="visible", timeout=3000)
        await ann(page, enriched, "Variabelen verrijkt met variable_metadata labels", side="right", color=AMBER)
        await pause(page, 2000)
        await clear_ann(page)
        await pause(page, 200)
    except Exception:
        pass

    # Kolomselectie modal openen
    kol_btn = page.get_by_role("button", name="Kolomselectie instellen →")
    try:
        await kol_btn.wait_for(state="visible", timeout=3000)
        await cursor_to(page, kol_btn)
        await ann(page, kol_btn, "Selecteer welke kolommen te decoderen / verrijken", side="left", color=ACCENT)
        await pause(page, 2000)
        await clear_ann(page)
        await kol_btn.click()
        await pause(page, 1200)

        # Modal zichtbaar
        modal_title = page.locator("text=Kolomselectie")
        await modal_title.wait_for(state="visible", timeout=4000)
        await ann(page, modal_title, "24 kolommen — vink aan wat je wilt decoderen", side="below", color=ACCENT)
        await pause(page, 2500)
        await clear_ann(page)

        # Sluit modal
        close = page.get_by_role("button", name="Klaar")
        await cursor_to(page, close)
        await pause(page, 400)
        await close.click()
        await pause(page, 800)
    except Exception:
        pass

    # Scroll naar Start Turbo Convert
    await smooth_scroll(page, 400)
    await pause(page, 400)

    start_btn = page.get_by_role("button", name="⚡ Start Turbo Convert ⚡")
    try:
        await start_btn.wait_for(state="visible", timeout=3000)
        await cursor_to(page, start_btn)
        await ann(page, start_btn, "Multiprocessing — alle bestanden tegelijk", side="above", color=GREEN)
        await pause(page, 2500)
        await clear_ann(page)
    except Exception:
        pass

    await pause(page, 400)


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
                "scale=1080:-1:flags=lanczos",
                "split[s0][s1]",
                "[s0]palettegen=max_colors=192:stats_mode=diff[p]",
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

        print("Recording…")
        await scene_home(page)
        await scene_upload(page)
        await scene_extract(page)
        await scene_validate(page)
        await scene_convert(page)

        await recorder.stop()
        await browser.close()


def main():
    frames_dir = Path(tempfile.mkdtemp(prefix="1cijferho-frames-"))
    try:
        asyncio.run(run(frames_dir))
        frame_count = len(list(frames_dir.glob("frame_*.png")))
        print(f"Captured {frame_count} frames ({frame_count / FPS:.0f}s)")
        print(f"Assembling → {OUT_GIF_SRC}")
        frames_to_gif(frames_dir, OUT_GIF_SRC)
        shutil.copy(OUT_GIF_SRC, OUT_GIF_DOCS)
        print(f"Done — {OUT_GIF_SRC.stat().st_size // 1024} KB")
    finally:
        shutil.rmtree(frames_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
