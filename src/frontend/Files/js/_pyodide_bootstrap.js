// Shared Pyodide bootstrap for the st_js pages. Loads Pyodide once per browser
// session (cached on window) and installs the cryptography package. Injected
// into the page scripts by the _load_js helper.
if (!window.__pyodidePromise) {
    window.__pyodidePromise = (async () => {
        const { loadPyodide } = await import("https://cdn.jsdelivr.net/pyodide/v0.24.1/full/pyodide.mjs");
        const py = await loadPyodide();
        await py.loadPackage(["micropip"]);
        await py.runPythonAsync(`
            import micropip
            await micropip.install('cryptography')
        `);
        return py;
    })();
}
const py = await window.__pyodidePromise;
