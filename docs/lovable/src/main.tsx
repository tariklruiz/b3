import { createRoot } from "react-dom/client";
import { HelmetProvider } from "react-helmet-async";
import App from "./App.tsx";
import "./index.css";

// Ensure <html> has data-theme set (the inline template script might have
// failed if, say, localStorage is unavailable). Then mirror it to .dark so
// Tailwind's dark tokens apply.
(function resyncTheme() {
  const root = document.documentElement;
  let theme = root.dataset.theme;
  if (theme !== "dark" && theme !== "light") {
    try {
      const stored = localStorage.getItem("fg-theme");
      theme = stored === "light" ? "light" : "dark";
    } catch {
      theme = "dark";
    }
    root.dataset.theme = theme;
  }
  if (theme === "dark") root.classList.add("dark");
  else root.classList.remove("dark");
})();

// Only mount React when we're on a fund route. Homepage at "/" is pure static
// HTML — mounting React there would render NotFound invisibly and log a 404
// to the console.
function shouldMountApp() {
  return window.location.pathname.startsWith("/fundo/");
}

function mount() {
  const el = document.getElementById("root");
  if (el && !el.dataset.mounted) {
    el.dataset.mounted = "1";
    createRoot(el).render(
      <HelmetProvider>
        <App />
      </HelmetProvider>
    );
  }
}

if (shouldMountApp()) {
  mount();
}

// BrowserRouter uses History API navigation (pushState / popstate), not hash
// changes. Listen for popstate so React mounts when the user navigates via
// back/forward buttons.
window.addEventListener("popstate", () => {
  if (shouldMountApp()) mount();
});
