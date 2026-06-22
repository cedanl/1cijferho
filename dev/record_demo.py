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
APP_URL  = "http://localhost:8502"
SIZE     = {"width": 1280, "height": 780}
ACCENT   = "#667eea"
GREEN    = "#22c55e"
AMBER    = "#f59e0b"
FPS      = 12

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
# Cursor — stapsgewijze animatie zodat beweging zichtbaar is in GIF
# ---------------------------------------------------------------------------

CURSOR_CSS = """
#__cur {
    position: fixed;
    pointer-events: none;
    z-index: 2147483646;
    width: 0; height: 0;
    border-style: solid;
    border-width: 0 7px 20px 7px;
    border-color: transparent transparent #1a1a1a transparent;
    transform: rotate(-30deg) skewY(10deg);
    filter: drop-shadow(1px 1px 2px rgba(255,255,255,.7));
    left: -40px; top: -40px;
}
"""


async def _ensure_cursor(page):
    await page.evaluate("""(css) => {
        if (document.getElementById('__cur')) return;
        const s = document.createElement('style');
        s.textContent = css;
        document.head.appendChild(s);
        const c = document.createElement('div');
        c.id = '__cur';
        document.body.appendChild(c);
    }""", CURSOR_CSS)


async def move_cursor(page, x: float, y: float, steps: int = 18, delay_ms: int = 30):
    """Animate cursor from current position to (x, y) in *steps* micro-moves."""
    await _ensure_cursor(page)
    await page.evaluate("""([tx, ty, steps, delay]) => new Promise(resolve => {
        const c = document.getElementById('__cur');
        const sx = parseFloat(c.style.left) || -40;
        const sy = parseFloat(c.style.top)  || -40;
        let i = 0;
        const t = setInterval(() => {
            i++;
            c.style.left = (sx + (tx - sx) * i / steps) + 'px';
            c.style.top  = (sy + (ty - sy) * i / steps) + 'px';
            if (i >= steps) { clearInterval(t); resolve(); }
        }, delay);
    })""", [x, y, steps, delay_ms])


async def cursor_to(page, locator):
    """Move cursor smoothly to centre of locator."""
    try:
        box = await locator.bounding_box(timeout=3000)
    except Exception:
        return
    if box:
        await move_cursor(page, box["x"] + box["width"] / 2 - 4,
                                box["y"] + box["height"] / 2 - 10)


# ---------------------------------------------------------------------------
# Annotations — enkel voor niet-vanzelfsprekende UI-elementen
# ---------------------------------------------------------------------------

ANN_CSS = (
    "@keyframes ann-in    { from { opacity:0; transform:scale(.85) } to { opacity:1; transform:scale(1) } }"
    "@keyframes ann-pulse { 0%,100% { box-shadow:0 0 0 0 var(--c) } 50% { box-shadow:0 0 0 8px transparent } }"
    ".__ar { animation: ann-in .2s ease, ann-pulse 1.5s .2s ease infinite }"
    ".__ab { animation: ann-in .2s ease }"
)


async def _inject_ann(page):
    await page.evaluate("""(css) => {
        if (document.getElementById('__ann-css')) return;
        const s = document.createElement('style');
        s.id = '__ann-css'; s.textContent = css;
        document.head.appendChild(s);
    }""", ANN_CSS)


async def ann(page, locator, label, side="right", color=ACCENT):
    await _inject_ann(page)
    if isinstance(locator, str):
        locator = page.locator(locator).first
    try:
        box = await locator.bounding_box(timeout=3000)
    except Exception:
        return
    if not box:
        return
    await page.evaluate("""([b, label, side, color]) => {
        document.querySelectorAll('.__ann').forEach(n => n.remove());
        const p = 8;
        const ring = document.createElement('div');
        ring.className = '__ann __ar';
        ring.style.cssText =
            '--c:' + color + '55;position:fixed;pointer-events:none;z-index:2147483647;'
          + 'left:'+(b.x-p)+'px;top:'+(b.y-p)+'px;'
          + 'width:'+(b.width+p*2)+'px;height:'+(b.height+p*2)+'px;'
          + 'border:2.5px solid '+color+';border-radius:6px;';
        document.body.appendChild(ring);
        const bw = Math.max(label.length * 7.2 + 20, 100);
        let bx, by;
        if (side==='right') { bx = b.x+b.width+12; by = b.y+b.height/2-13; }
        if (side==='left')  { bx = b.x-bw-12;      by = b.y+b.height/2-13; }
        if (side==='above') { bx = b.x;             by = b.y-34; }
        if (side==='below') { bx = b.x;             by = b.y+b.height+10; }
        const badge = document.createElement('div');
        badge.className = '__ann __ab';
        badge.textContent = label;
        badge.style.cssText =
            'position:fixed;pointer-events:none;z-index:2147483647;'
          + 'left:'+bx+'px;top:'+by+'px;background:'+color+';color:#fff;'
          + 'padding:3px 10px;font:bold 11.5px/22px monospace;'
          + 'border-radius:3px;white-space:nowrap;box-shadow:0 2px 8px rgba(0,0,0,.4);';
        document.body.appendChild(badge);
    }""", [box, label, side, color])


async def clear_ann(page):
    await page.evaluate("() => document.querySelectorAll('.__ann').forEach(n => n.remove())")


async def pause(page, ms):
    await page.wait_for_timeout(ms)


async def wait_streamlit(page):
    await page.wait_for_selector('[data-testid="stAppViewContainer"]', timeout=20_000)
    await pause(page, 1000)


async def smooth_scroll(page, distance: int):
    steps = max(distance // 40, 6)
    step  = distance // steps
    for _ in range(steps):
        await page.evaluate(f"() => window.scrollBy(0, {step})")
        await pause(page, 80)


# ---------------------------------------------------------------------------
# Demo scenes — langzaam, natuurlijk, max 4 annotaties totaal
# ---------------------------------------------------------------------------

async def scene_home(page):
    await page.goto(APP_URL)
    await wait_streamlit(page)
    await pause(page, 800)

    # Laat de pagina even ademen voordat we iets doen
    await pause(page, 1500)

    # Cursor traag naar "Probeer met demo" — annoteer de knop zelf
    demo_btn = page.get_by_role("button", name="Probeer met demo")
    await cursor_to(page, demo_btn)
    await pause(page, 600)
    # ANNOTATIE 1: waarom demo kiezen
    await ann(page, demo_btn, "Geen installatie — werkt direct met voorbeelddata", side="left", color=GREEN)
    await pause(page, 3000)
    await clear_ann(page)
    await pause(page, 300)
    await demo_btn.click()
    await pause(page, 1500)


async def scene_upload(page):
    await wait_streamlit(page)
    await pause(page, 800)

    # Cursor naar de groene status-balk — dat is de interessante info
    status = page.locator("text=26 bestanden gevonden").first
    try:
        await status.wait_for(state="visible", timeout=4000)
        await cursor_to(page, status)
        await pause(page, 600)
        # ANNOTATIE 2: wat er gevonden is
        await ann(page, status, "3 types herkend: beschrijvingen · Dec-tabellen · data", side="above", color=GREEN)
        await pause(page, 3000)
        await clear_ann(page)
        await pause(page, 500)
    except Exception:
        pass

    # Klap "Bekijk 3 bestanden" open zodat de bestandsnamen zichtbaar zijn
    first_dropdown = page.locator('[data-testid="stSelectbox"], details, summary').first
    bekijk = page.locator("text=Bekijk 3 bestanden").first
    try:
        await bekijk.wait_for(state="visible", timeout=3000)
        await cursor_to(page, bekijk)
        await pause(page, 500)
        await bekijk.click()
        await pause(page, 1200)
    except Exception:
        pass

    # Cursor naar "Ga door naar stap 1 →"
    fwd = page.get_by_role("button", name="Ga door naar stap 1 →")
    try:
        await fwd.wait_for(state="visible", timeout=3000)
        await cursor_to(page, fwd)
        await pause(page, 600)
        await fwd.click()
    except Exception:
        await page.get_by_text("Stap 1 · Metadata extraheren").click()
    await pause(page, 1500)


async def scene_extract(page):
    await wait_streamlit(page)
    await pause(page, 800)

    # Cursor naar knop en klik — geen annotatie, de actie spreekt voor zich
    run_btn = page.get_by_role("button", name="Extraheren starten")
    try:
        await run_btn.wait_for(state="visible", timeout=3000)
        await cursor_to(page, run_btn)
        await pause(page, 800)
        await run_btn.click()
        # Console log opent vanzelf — wacht tot die zichtbaar is en laat hem lezen
        await pause(page, 2000)
        console_text = page.locator("text=Extractie gestart").first
        try:
            await console_text.wait_for(state="visible", timeout=6000)
            await pause(page, 4000)   # laat de log uitrollen en leesbaar zijn
        except Exception:
            await pause(page, 6000)
    except Exception:
        pass

    fwd = page.get_by_role("button", name="Ga door naar stap 2 →")
    try:
        await fwd.wait_for(state="visible", timeout=5000)
        await cursor_to(page, fwd)
        await pause(page, 500)
        await fwd.click()
    except Exception:
        await page.get_by_text("Stap 2 · Metadata valideren").click()
    await pause(page, 1500)


async def scene_validate(page):
    await wait_streamlit(page)
    await pause(page, 800)

    validate_btn = page.get_by_role("button", name="Validatie starten")
    try:
        await validate_btn.wait_for(state="visible", timeout=3000)
        await cursor_to(page, validate_btn)
        await pause(page, 700)
        await validate_btn.click()
        await pause(page, 2000)
        # Wacht op console log — laat resultaat zichtbaar zijn
        passed = page.locator("text=18 passed").first
        try:
            await passed.wait_for(state="visible", timeout=8000)
            await pause(page, 1000)
            # ANNOTATIE 3: het resultaat is niet vanzelfsprekend voor nieuwe gebruikers
            await ann(page, passed, "Alle veldposities correct — klaar voor conversie", side="right", color=GREEN)
            await pause(page, 3000)
            await clear_ann(page)
        except Exception:
            await pause(page, 5000)
    except Exception:
        pass

    fwd = page.get_by_role("button", name="Ga door naar stap 3 →")
    try:
        await fwd.wait_for(state="visible", timeout=5000)
        await cursor_to(page, fwd)
        await pause(page, 500)
        await fwd.click()
    except Exception:
        await page.get_by_text("Stap 3 · Turbo Conversie").click()
    await pause(page, 1500)


async def scene_convert(page):
    await wait_streamlit(page)
    await pause(page, 800)

    # Scroll traag naar uitvoervarianten — toon de opties
    await smooth_scroll(page, 320)
    await pause(page, 800)

    # _decoded label: annoteert wat het betekent
    decoded = page.locator("text=_decoded").first
    try:
        await decoded.wait_for(state="visible", timeout=3000)
        await cursor_to(page, decoded)
        await pause(page, 400)
        await ann(page, decoded, "Codes vertaald naar leesbare omschrijvingen via Dec_* tabellen", side="right", color=AMBER)
        await pause(page, 3000)
        await clear_ann(page)
        await pause(page, 400)
    except Exception:
        pass

    # Kolomselectie openen — ANNOTATIE 4
    kol_btn = page.get_by_role("button", name="Kolomselectie instellen →")
    try:
        await kol_btn.wait_for(state="visible", timeout=3000)
        await cursor_to(page, kol_btn)
        await pause(page, 600)
        await ann(page, kol_btn, "Kies zelf welke kolommen te decoderen", side="left", color=ACCENT)
        await pause(page, 2500)
        await clear_ann(page)
        await kol_btn.click()
        await pause(page, 1200)

        # Toon modal — laat het even zien
        modal = page.locator("text=Kolomselectie").first
        try:
            await modal.wait_for(state="visible", timeout=4000)
            await pause(page, 2500)
        except Exception:
            pass

        close = page.get_by_role("button", name="Klaar")
        await cursor_to(page, close)
        await pause(page, 400)
        await close.click()
        await pause(page, 800)
    except Exception:
        pass

    # Scroll naar Start-knop
    await smooth_scroll(page, 350)
    await pause(page, 600)

    start = page.get_by_role("button", name="⚡ Start Turbo Convert ⚡")
    try:
        await start.wait_for(state="visible", timeout=3000)
        await cursor_to(page, start)
        await pause(page, 800)
        # Hover effect — even wachten voor je klikt zodat het reëel lijkt
        await pause(page, 1200)
    except Exception:
        pass

    await pause(page, 500)


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
        page    = await browser.new_page(viewport=SIZE)

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
        n = len(list(frames_dir.glob("frame_*.png")))
        print(f"Captured {n} frames ({n / FPS:.0f}s)")
        print(f"Assembling → {OUT_GIF_SRC}")
        frames_to_gif(frames_dir, OUT_GIF_SRC)
        shutil.copy(OUT_GIF_SRC, OUT_GIF_DOCS)
        print(f"Done — {OUT_GIF_SRC.stat().st_size // 1024} KB")
    finally:
        shutil.rmtree(frames_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
