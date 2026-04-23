import { createRoot } from "react-dom/client";
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

// Only mount React when we're on a hash route (fund page).
// Homepage at "/" is pure static HTML — mounting React would render NotFound
// invisibly and log a 404 to the console.
function shouldMountApp() {
  const hash = window.location.hash || "";
  return /^#\/\w/.test(hash);
}

function mount() {
  const el = document.getElementById("root");
  if (el && !el.dataset.mounted) {
    el.dataset.mounted = "1";
    createRoot(el).render(<App />);
  }
}

if (shouldMountApp()) {
  mount();
}

window.addEventListener("hashchange", () => {
  if (shouldMountApp()) mount();
});


