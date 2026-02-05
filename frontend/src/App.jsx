import React, { useEffect, useState } from "react";
import { api } from "./api.js";

function GlassCard({ title, children, actions }) {
  return (
    <div className="glass">
      {title && <div className="card-title">{title}</div>}
      <div className="card-body">{children}</div>
      {actions && <div className="card-actions">{actions}</div>}
    </div>
  );
}

function formatDate(d) {
  return new Date(d).toISOString().slice(0, 10);
}

function getTelegramUserId() {
  return window?.Telegram?.WebApp?.initDataUnsafe?.user?.id || null;
}

export default function App() {
  const [loading, setLoading] = useState(true);
  const [telegramId, setTelegramId] = useState(api.getTelegramId() || "");
  const [linkCode, setLinkCode] = useState("");
  const [habits, setHabits] = useState([]);
  const [friends, setFriends] = useState([]);
  const [error, setError] = useState("");
  const [panel, setPanel] = useState("home");
  const [selected, setSelected] = useState(null);
  const [stats, setStats] = useState(null);
  const [calendar, setCalendar] = useState(null);
  const [statsCalendar, setStatsCalendar] = useState(null);
  const [month, setMonth] = useState(formatDate(new Date()).slice(0, 7));
  const [newHabit, setNewHabit] = useState({
    name: "",
    start_date: formatDate(new Date()),
    end_date: "",
    reminder_time: ""
  });

  const isLinked = !!telegramId;

  const loadAll = async () => {
    setLoading(true);
    try {
      const own = await api.listHabits();
      const fr = await api.listFriendHabits();
      setHabits(own);
      setFriends(fr);
      setError("");
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const tgId = getTelegramUserId();
    if (tgId) {
      const id = String(tgId);
      setTelegramId(id);
      api.setTelegramId(id);
    }
  }, []);

  useEffect(() => {
    if (!isLinked) {
      setLoading(false);
      return;
    }
    loadAll();
  }, [isLinked]);

  const onLinkById = async () => {
    setError("");
    if (!telegramId) return;
    api.setTelegramId(telegramId);
    await loadAll();
  };

  const onLinkByCode = async () => {
    setError("");
    try {
      const data = await api.linkResolve(linkCode.trim());
      const tg = String(data.telegram_user_id);
      setTelegramId(tg);
      api.setTelegramId(tg);
      await loadAll();
    } catch (err) {
      setError(err.message);
    }
  };

  const onCreateHabit = async () => {
    setError("");
    try {
      await api.createHabit({
        name: newHabit.name,
        start_date: newHabit.start_date,
        end_date: newHabit.end_date || null,
        reminder_time: newHabit.reminder_time || null
      });
      setNewHabit({
        name: "",
        start_date: formatDate(new Date()),
        end_date: "",
        reminder_time: ""
      });
      await loadAll();
      setPanel("habits");
    } catch (err) {
      setError(err.message);
    }
  };

  const onCheckin = async (id) => {
    await api.checkin(id);
    await loadAll();
  };

  const onSkip = async (id) => {
    await api.skip(id);
    await loadAll();
  };

  const onDelete = async (id) => {
    await api.deleteHabit(id);
    await loadAll();
  };

  const openStats = async (habit) => {
    setSelected(habit);
    const data = await api.habitStats(habit.id);
    setStats(data);
    const cal = await api.habitCalendar(habit.id, month);
    setStatsCalendar(cal);
    setPanel("stats");
  };

  const openCalendar = async (habit) => {
    setSelected(habit);
    const data = await api.habitCalendar(habit.id, month);
    setCalendar(data);
    setPanel("calendar");
  };

  const reloadCalendar = async () => {
    if (!selected) return;
    const data = await api.habitCalendar(selected.id, month);
    setCalendar(data);
  };

  const reloadStatsCalendar = async () => {
    if (!selected) return;
    const data = await api.habitCalendar(selected.id, month);
    setStatsCalendar(data);
  };

  const buildCalendarGrid = (monthStr, data) => {
    if (!data) return [];
    const [y, m] = monthStr.split("-").map((v) => parseInt(v, 10));
    const first = new Date(y, m - 1, 1);
    const daysInMonth = new Date(y, m, 0).getDate();
    const mondayIndex = (first.getDay() + 6) % 7;
    const marked = new Set(data.marked || []);
    const skipped = new Set(data.skipped || []);
    const cells = [];
    for (let i = 0; i < mondayIndex; i += 1) cells.push({ type: "empty" });
    for (let d = 1; d <= daysInMonth; d += 1) {
      const dayStr = `${monthStr}-${String(d).padStart(2, "0")}`;
      let status = "none";
      if (marked.has(dayStr)) status = "done";
      if (skipped.has(dayStr)) status = "skip";
      cells.push({ type: "day", day: d, status });
    }
    return cells;
  };

  const buildMiniBars = (monthStr, data) => {
    if (!data) return [];
    const [y, m] = monthStr.split("-").map((v) => parseInt(v, 10));
    const daysInMonth = new Date(y, m, 0).getDate();
    const marked = new Set(data.marked || []);
    const skipped = new Set(data.skipped || []);
    const bars = [];
    for (let d = Math.max(daysInMonth - 13, 1); d <= daysInMonth; d += 1) {
      const dayStr = `${monthStr}-${String(d).padStart(2, "0")}`;
      let status = "none";
      if (marked.has(dayStr)) status = "done";
      if (skipped.has(dayStr)) status = "skip";
      bars.push({ day: d, status });
    }
    return bars;
  };

  if (loading) {
    return (
      <div className="screen">
        <div className="logo">Habit</div>
        <div className="muted">Загрузка...</div>
      </div>
    );
  }

  if (!isLinked) {
    return (
      <div className="screen">
        <div className="logo">Habit</div>
        <GlassCard title="Подключить Telegram">
          <div className="field">
            <label>Код из бота</label>
            <input
              value={linkCode}
              onChange={(e) => setLinkCode(e.target.value)}
              placeholder="например 123456"
            />
          </div>
          <div className="row">
            <button className="primary" onClick={onLinkByCode}>
              Связать по коду
            </button>
          </div>
          <div className="field">
            <label>Или введи Telegram user_id</label>
            <input
              value={telegramId}
              onChange={(e) => setTelegramId(e.target.value)}
              placeholder="например 123456789"
            />
            <button className="ghost" onClick={onLinkById}>
              Использовать user_id
            </button>
          </div>
          {error && <div className="error">{error}</div>}
        </GlassCard>
      </div>
    );
  }

  return (
    <div className="screen">
      <header className="topbar">
        <div className="logo">Habit</div>
        <div className="badge">TG {telegramId}</div>
      </header>

      <nav className="tabs">
        {[
          { key: "home", label: "Сегодня" },
          { key: "habits", label: "Мои" },
          { key: "friends", label: "Друзья" },
          { key: "stats", label: "Статистика" },
          { key: "calendar", label: "Календарь" },
          { key: "create", label: "Новая" }
        ].map((t) => (
          <button
            key={t.key}
            className={panel === t.key ? "tab active" : "tab"}
            onClick={() => setPanel(t.key)}
          >
            {t.label}
          </button>
        ))}
      </nav>

      <div className="grid">
        {panel === "home" && (
          <GlassCard title="Сегодня">
            <div className="muted">
              Нажимай ✅, чтобы отметить, или ⏭️, чтобы пропустить.
            </div>
            {habits.length === 0 && (
              <div className="muted">Добавь первую привычку</div>
            )}
            {habits.map((h) => (
              <div className="habit" key={`today-${h.id}`}>
                <div className="habit-name">{h.name}</div>
                <div className="habit-actions">
                  <button onClick={() => onCheckin(h.id)}>✅</button>
                  <button onClick={() => onSkip(h.id)}>⏭️</button>
                </div>
              </div>
            ))}
          </GlassCard>
        )}

        {panel === "habits" && (
          <GlassCard title="Мои привычки">
            {habits.length === 0 && (
              <div className="muted">Пока нет привычек</div>
            )}
            {habits.map((h) => (
              <div className="habit" key={h.id}>
                <div className="habit-name">{h.name}</div>
                <div className="habit-actions">
                  <button onClick={() => onCheckin(h.id)}>✅</button>
                  <button onClick={() => onSkip(h.id)}>⏭️</button>
                  <button onClick={() => openStats(h)}>📊</button>
                  <button onClick={() => openCalendar(h)}>📅</button>
                  <button onClick={() => onDelete(h.id)}>🗑️</button>
                </div>
              </div>
            ))}
          </GlassCard>
        )}

        {panel === "friends" && (
          <GlassCard title="Привычки друзей">
            {friends.length === 0 && (
              <div className="muted">Нет доступов от друзей</div>
            )}
            {friends.map((h) => (
              <div className="habit" key={`f-${h.id}`}>
                <div className="habit-name">{h.name}</div>
                <div className="habit-actions">
                  <button onClick={() => openStats(h)}>📊</button>
                  <button onClick={() => openCalendar(h)}>📅</button>
                </div>
              </div>
            ))}
          </GlassCard>
        )}

        {panel === "stats" && (
          <GlassCard title="Статистика">
            {!selected && <div className="muted">Выбери привычку</div>}
            {selected && stats && (
              <div className="stats">
                <div className="stat-title">{selected.name}</div>
                <div className="stat-row">Всего отметок: {stats.total}</div>
                <div className="stat-row">
                  Выполнение: {stats.completion}%
                  <div className="bar">
                    <span style={{ width: `${stats.completion}%` }} />
                  </div>
                </div>
                <div className="stat-row">
                  За месяц: {stats.month_completion}%
                  <div className="bar">
                    <span style={{ width: `${stats.month_completion}%` }} />
                  </div>
                </div>
                <div className="stat-row small">
                  Последние 14 дней
                  <div className="mini-bars">
                    {buildMiniBars(month, statsCalendar).map((b) => (
                      <div
                        key={`b-${b.day}`}
                        className={`mini-bar ${b.status}`}
                        title={`${b.day}`}
                      />
                    ))}
                  </div>
                </div>
                <div className="row">
                  <input
                    type="month"
                    value={month}
                    onChange={(e) => setMonth(e.target.value)}
                  />
                  <button className="ghost" onClick={reloadStatsCalendar}>
                    Обновить
                  </button>
                </div>
              </div>
            )}
          </GlassCard>
        )}

        {panel === "calendar" && (
          <GlassCard title="Календарь">
            {!selected && <div className="muted">Выбери привычку</div>}
            {selected && (
              <>
                <div className="row">
                  <input
                    type="month"
                    value={month}
                    onChange={(e) => setMonth(e.target.value)}
                  />
                  <button className="ghost" onClick={reloadCalendar}>
                    Показать
                  </button>
                </div>
                {calendar && (
                  <>
                    <div className="muted">
                      Отмечено: {calendar.marked.length} · Пропусков: {calendar.skipped.length}
                    </div>
                    <div className="calendar-grid">
                      {buildCalendarGrid(month, calendar).map((c, idx) => {
                        if (c.type === "empty") {
                          return <div className="cal-cell empty" key={`e-${idx}`} />;
                        }
                        return (
                          <div className={`cal-cell ${c.status}`} key={`d-${c.day}`}>
                            <span className="cal-day">{c.day}</span>
                          </div>
                        );
                      })}
                    </div>
                    <div className="legend">
                      <span className="legend-item done">✅ выполнено</span>
                      <span className="legend-item skip">⏭️ пропуск</span>
                      <span className="legend-item none">⬜ нет</span>
                    </div>
                  </>
                )}
              </>
            )}
          </GlassCard>
        )}

        {panel === "create" && (
          <GlassCard title="Новая привычка">
            <div className="field">
              <label>Название</label>
              <input
                value={newHabit.name}
                onChange={(e) =>
                  setNewHabit({ ...newHabit, name: e.target.value })
                }
              />
            </div>
            <div className="row">
              <div className="field">
                <label>Старт</label>
                <input
                  type="date"
                  value={newHabit.start_date}
                  onChange={(e) =>
                    setNewHabit({ ...newHabit, start_date: e.target.value })
                  }
                />
              </div>
              <div className="field">
                <label>Конец</label>
                <input
                  type="date"
                  value={newHabit.end_date}
                  onChange={(e) =>
                    setNewHabit({ ...newHabit, end_date: e.target.value })
                  }
                />
              </div>
            </div>
            <div className="field">
              <label>Напоминание (HH:MM)</label>
              <input
                placeholder="09:00"
                value={newHabit.reminder_time}
                onChange={(e) =>
                  setNewHabit({ ...newHabit, reminder_time: e.target.value })
                }
              />
            </div>
            <button className="primary" onClick={onCreateHabit}>
              Создать
            </button>
          </GlassCard>
        )}
      </div>
    </div>
  );
}
