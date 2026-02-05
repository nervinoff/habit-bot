const API_URL = import.meta.env.VITE_API_URL || "http://127.0.0.1:8000";

function getTelegramId() {
  const tg = window?.Telegram?.WebApp?.initDataUnsafe?.user?.id;
  if (tg) {
    localStorage.setItem("telegram_user_id", String(tg));
    return String(tg);
  }
  return localStorage.getItem("telegram_user_id");
}

function setTelegramId(id) {
  localStorage.setItem("telegram_user_id", String(id));
}

function clearTelegramId() {
  localStorage.removeItem("telegram_user_id");
}

async function request(path, options = {}) {
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {})
  };
  const tg = getTelegramId();
  if (tg && !headers["X-Telegram-Id"]) {
    headers["X-Telegram-Id"] = tg;
  }
  let url = `${API_URL}${path}`;
  if (tg) {
    const joiner = url.includes("?") ? "&" : "?";
    url = `${url}${joiner}telegram_user_id=${encodeURIComponent(tg)}`;
  }
  const doFetch = async () => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 25000);
    try {
      return await fetch(url, {
        ...options,
        headers,
        signal: controller.signal
      });
    } finally {
      clearTimeout(timeoutId);
    }
  };
  let res;
  try {
    res = await doFetch();
  } catch (err) {
    // Retry once on abort (Render cold start / slow wake)
    if (err?.name === "AbortError") {
      res = await doFetch();
    } else {
      throw err;
    }
  }
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Request failed");
  }
  if (res.status === 204) return null;
  return res.json();
}

export const api = {
  getTelegramId,
  setTelegramId,
  clearTelegramId,
  linkResolve: (code) =>
    request("/link/resolve", {
      method: "POST",
      body: JSON.stringify({ code })
    }),
  listHabits: () => request("/habits"),
  listFriendHabits: () => request("/friends/habits"),
  createHabit: (payload) =>
    request("/habits", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  habitStats: (id) => request(`/habits/${id}/stats`),
  habitCalendar: (id, month) => request(`/habits/${id}/calendar?month=${month}`),
  checkin: (id, day = null) =>
    request(`/habits/${id}/checkin`, {
      method: "POST",
      body: JSON.stringify({ day })
    }),
  skip: (id, day = null) =>
    request(`/habits/${id}/skip`, {
      method: "POST",
      body: JSON.stringify({ day })
    }),
  deleteHabit: (id) => request(`/habits/${id}`, { method: "DELETE" })
};
