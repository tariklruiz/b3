import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";

// Mirror html[data-theme] to .dark class so Tailwind's dark tokens apply.
if (document.documentElement.dataset.theme === "dark") {
  document.documentElement.classList.add("dark");
}

// Only mount React when we're on a hash route (fund page).
// Homepage at "/" is pure static HTML — mounting React there would just render
// NotFound invisibly and log a 404 to the console.
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

// If the user pastes a fund URL after arriving via "/", catch the hash change.
window.addEventListener("hashchange", () => {
  if (shouldMountApp()) mount();
});

