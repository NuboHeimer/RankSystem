//----------------------------------------------------------------------------
//   Module:       RankSystem
//   Author:       NuboHeimer (https://live.vkvideo.ru/nuboheimer)
//   Email:        nuboheimer@yandex.ru
//   Help:         https://t.me/nuboheimersb/5
//----------------------------------------------------------------------------

//   Version:      0.11.0

using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Threading;
using System.Data;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.IO;
using Newtonsoft.Json;
using System.Windows.Forms;
using System.Drawing;
using System.ComponentModel;

// ============================================================================
// КОНСТАНТЫ И КОНФИГУРАЦИЯ
// ============================================================================

// Константы и настройки системы рангов
public static class RankSystemConfig
{
    // Время по умолчанию для добавления к просмотру (секунды)
    public const int DEFAULT_TIME_TO_ADD = 60;

    // Количество монет по умолчанию для добавления
    public const long DEFAULT_COINS_TO_ADD = 0;

    // Количество позиций в топе по умолчанию
    public const int DEFAULT_TOP_COUNT = 3;

    // Путь к файлу базы данных
    public const string DB_PATH = "RankSystem.db";

    // Таймаут для операций с базой данных (миллисекунды)
    public const int DB_TIMEOUT = 5000;

    // Размер кэша базы данных (страницы)
    public const int DB_CACHE_SIZE = -2000;
}

// ============================================================================
// МОДЕЛИ ДАННЫХ
// ============================================================================

// Класс для десериализации данных из Live.json из MiniChat
public class LiveData
{
    public string Type { get; set; }
    public string Service { get; set; }
    public string Date { get; set; }
    public string ID { get; set; }
    public string UserID { get; set; }
    public string UserName { get; set; }
    public AvatarData Avatar { get; set; }
}

// Данные аватара пользователя
public class AvatarData
{
    public string Default { get; set; }
    public string Large { get; set; }
}

// Основная модель пользователя в системе рангов
public class UserData
{
    public string UUID { get; set; }
    public string Service { get; set; }
    public string ServiceUserId { get; set; }
    public string UserName { get; set; }
    public long WatchTime { get; set; }
    public DateTime FollowDate { get; set; } = DateTime.MinValue;
    public long MessageCount { get; set; }
    public long Coins { get; set; }
    public string GameWhenFollow { get; set; }
}

// История изменений имени пользователя
public class UserNameHistory
{
    public long Id { get; set; }
    public string UUID { get; set; }
    public string Service { get; set; }
    public string ServiceUserId { get; set; }
    public string OldUserName { get; set; }
    public string NewUserName { get; set; }
    public DateTime ChangeDate { get; set; }
}

// Ежедневная статистика пользователя
public class DailyStats
{
    public long Id { get; set; }
    public string UUID { get; set; }
    public string Date { get; set; } // YYYY-MM-DD формат
    public string ServiceUserId { get; set; }
    public string Service { get; set; }
    public long WatchTime { get; set; }
    public long MessageCount { get; set; }
    public long Coins { get; set; }
    public long SpentCoins { get; set; }
}

// ============================================================================
// ОСНОВНОЙ КЛАСС CPHInline
// ============================================================================

// Содержит публичные методы для вызова из Streamer.bot
public class CPHInline
{
    // Инициализация системы рангов
    // Создает базу данных и необходимые таблицы
    public void Init()
    {
        DatabaseManager.InitializeDatabase();
    }

    // Удаление базы данных
    // Полностью очищает все данные и пересоздает структуру
    // Возвращает: Всегда true
    public bool DropDatabase()
    {
        DatabaseManager.DropDatabase();
        return true;
    }

    // Добавление сообщения пользователю
    // Увеличивает счетчик сообщений и добавляет монеты
    // Возвращает: true если операция успешна, false при ошибке
    public bool AddMessageCount()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = RankSystemInternal.CreateUserFromArgs(this, service);
            if (string.IsNullOrEmpty(user.Service) || string.IsNullOrEmpty(user.ServiceUserId))
            {
                CPH.LogError($"[RankSystem][AddMessageCount] Critical user data missing. Service: {user.Service}, ServiceUserId: {user.ServiceUserId}");
                return false;
            }

            var existingUser = RankSystemInternal.GetExistingUser(user.Service, user.ServiceUserId);
            if (existingUser is not null)
            {
                // Сохраняем актуальное имя пользователя из аргументов
                string newUserName = user.UserName;
                user = existingUser;
                user.MessageCount += 1;
                // Обновляем имя пользователя, если оно изменилось
                if (!string.IsNullOrEmpty(newUserName) && !string.Equals(user.UserName, newUserName, StringComparison.OrdinalIgnoreCase))
                {
                    user.UserName = newUserName;
                }
            }
            else
            {
                user.MessageCount = 1;
            }

            if (!CPH.TryGetArg("coinsToAdd", out long coinsToAdd))
                coinsToAdd = RankSystemConfig.DEFAULT_COINS_TO_ADD;
            user.Coins += coinsToAdd;
            DatabaseManager.UpsertUser(user);

            // Обновляем дневную статистику
            string today = DateTime.Now.ToString("yyyy-MM-dd");
            DatabaseManager.AddToDailyStatsInternal(user.Service, user.ServiceUserId, today, messageCount: 1, coins: coinsToAdd);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem][AddMessageCount] Error: {ex}");
            return false;
        }
    }

    // Добавление времени просмотра для списка пользователей
    // Обновляет время просмотра и добавляет монеты для всех пользователей в списке
    // Возвращает: true если операция успешна, false при ошибке
    public bool AddWatchTime()
    {
        try
        {
            if (!args.ContainsKey("users"))
            {
                CPH.LogWarn("[RankSystem][AddWatchTime] Список пользователей пуст или отсутствует.");
                return false;
            }

            var currentViewers = (List<Dictionary<string, object>>)args["users"];
            if (currentViewers.Count == 0)
            {
                CPH.LogWarn("[RankSystem][AddWatchTime] Список пользователей пуст.");
                return false;
            }

            string service = RankSystemInternal.NormalizeService(this);
            if (!CPH.TryGetArg("timeToAdd", out int timeToAdd))
                timeToAdd = RankSystemConfig.DEFAULT_TIME_TO_ADD;
            foreach (var viewer in currentViewers)
            {
                string userName = viewer["userName"].ToString().ToLower();
                string userId = viewer["id"].ToString();
                var user = RankSystemInternal.CreateUserFromArgs(this, service, userName, userId);
                var existingUser = RankSystemInternal.GetExistingUser(user.Service, user.ServiceUserId);
                if (existingUser is not null)
                {
                    // Сохраняем актуальное имя пользователя из аргументов
                    string newUserName = user.UserName;
                    user = existingUser;
                    // Обновляем имя пользователя, если оно изменилось
                    if (!string.IsNullOrEmpty(newUserName) && !string.Equals(user.UserName, newUserName, StringComparison.OrdinalIgnoreCase))
                    {
                        user.UserName = newUserName;
                    }
                }
                if (!CPH.TryGetArg("coinsToAdd", out long coinsToAdd))
                    coinsToAdd = RankSystemConfig.DEFAULT_COINS_TO_ADD;
                user.Coins += coinsToAdd;
                user.WatchTime += timeToAdd;
                DatabaseManager.UpsertUser(user);

                // Обновляем дневную статистику
                string today = DateTime.Now.ToString("yyyy-MM-dd");
                DatabaseManager.AddToDailyStatsInternal(user.Service, user.ServiceUserId, today, watchTime: timeToAdd, coins: coinsToAdd);
            }

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"Ошибка в AddWatchTime: {ex}");
            return false;
        }
    }

    // Добавление даты подписки пользователя
    // Устанавливает дату подписки и добавляет монеты
    // Возвращает: true если операция успешна, false при ошибке
    public bool AddFollowDate()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = RankSystemInternal.CreateUserFromArgs(this, service);
            var existingUser = RankSystemInternal.GetExistingUser(user.Service, user.ServiceUserId);
            if (existingUser is not null)
            {
                // Сохраняем актуальное имя пользователя из аргументов
                string newUserName = user.UserName;
                user = existingUser;
                // Обновляем имя пользователя, если оно изменилось
                if (!string.IsNullOrEmpty(newUserName) && !string.Equals(user.UserName, newUserName, StringComparison.OrdinalIgnoreCase))
                {
                    user.UserName = newUserName;
                }
            }
            if (!CPH.TryGetArg("coinsToAdd", out long coinsToAdd))
                coinsToAdd = RankSystemConfig.DEFAULT_COINS_TO_ADD;
            if (CPH.TryGetArg("game", out string game))
                user.GameWhenFollow = game; // записываем категорию стрима, если она есть аргументах.
            if (!CPH.TryGetArg("minichat.Data.Date", out DateTime followDate))
                followDate = DateTime.Now;
            user.FollowDate = followDate;
            user.Coins += coinsToAdd;
            DatabaseManager.UpsertUser(user);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] AddFollowDate Error: {ex}");
            return false;
        }
    }

    // Получение количества сообщений пользователя
    // Возвращает: true если операция успешна, false при ошибке
    public bool GetMessageCount()
    {
        try
        {
            string targetUser = RankSystemInternal.NormalizeTargetUser(args["rawInput"].ToString());
            if (targetUser.Equals(""))
            {
                long messageCount = RankSystemInternal.GetMessageCount(this);
                CPH.SetArgument("messageCount", messageCount);
            }
            else
            {
                long messageCount = RankSystemInternal.GetMessageCount(this, targetUser);
                CPH.SetArgument("messageCount", messageCount);
                CPH.SetArgument("userName", targetUser);

            }

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetMessageCount Error: {ex}");
            return false;
        }
    }

    // Получение времени просмотра пользователя
    // Возвращает: true если операция успешна, false при ошибке
    public bool GetWatchTime()
    {
        try
        {
            string targetUser = RankSystemInternal.NormalizeTargetUser(args["rawInput"].ToString());
            if (targetUser.Equals(""))
            {
                long watchTime = RankSystemInternal.GetWatchTime(this);
                CPH.SetArgument("watchTime", watchTime == 0 ? 0 : RankSystemInternal.FormatDateTime(watchTime));
            }
            else
            {
                long watchTime = RankSystemInternal.GetWatchTime(this, targetUser);
                CPH.SetArgument("watchTime", watchTime == 0 ? 0 : RankSystemInternal.FormatDateTime(watchTime));
                CPH.SetArgument("userName", targetUser);
            }

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetWatchTime Error: {ex}");
            return false;
        }
    }

    public bool GetFollowDate()
    {
        try
        {
            string targetUser = RankSystemInternal.NormalizeTargetUser(args["rawInput"].ToString());
            if (targetUser.Equals(""))
            {
                DateTime followDate = RankSystemInternal.GetFollowDate(this);
                CPH.SetArgument("followDate", followDate == DateTime.MinValue ? "неизвестно когда" : followDate.ToString("dd.MM.yyyy HH:mm"));
            }
            else
            {
                DateTime followDate = RankSystemInternal.GetFollowDate(this, targetUser);
                CPH.SetArgument("followDate", followDate == DateTime.MinValue ? "неизвестно когда" : followDate.ToString("dd.MM.yyyy HH:mm"));
                CPH.SetArgument("userName", targetUser);
            }

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetFollowDate Error: {ex}");
            return false;
        }
    }

    public bool GetGameWhenFollow()
    {
        try
        {
            string targetUser = RankSystemInternal.NormalizeTargetUser(args["rawInput"].ToString());
            if (targetUser.Equals(""))
            {
                string gameWhenFollow = RankSystemInternal.GetGameWhenFollow(this);
                CPH.SetArgument("gameWhenFollow", string.IsNullOrEmpty(gameWhenFollow) ? "игры нет" : gameWhenFollow);
            }
            else
            {
                string gameWhenFollow = RankSystemInternal.GetGameWhenFollow(this, targetUser);
                CPH.SetArgument("gameWhenFollow", string.IsNullOrEmpty(gameWhenFollow) ? "игры нет" : gameWhenFollow);
                CPH.SetArgument("userName", targetUser);
            }

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetGameWhenFollow Error: {ex}");
            return false;
        }
    }

    public bool AddCoins()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = RankSystemInternal.CreateUserFromArgs(this, service);
            var existingUser = RankSystemInternal.GetExistingUser(user.Service, user.ServiceUserId);
            if (existingUser is not null)
            {
                // Сохраняем актуальное имя пользователя из аргументов
                string newUserName = user.UserName;
                user = existingUser;
                // Обновляем имя пользователя, если оно изменилось
                if (!string.IsNullOrEmpty(newUserName) && !string.Equals(user.UserName, newUserName, StringComparison.OrdinalIgnoreCase))
                {
                    user.UserName = newUserName;
                }
            }
            long coinsFromArgs = RankSystemConfig.DEFAULT_COINS_TO_ADD;
            if (!CPH.TryGetArg("coinsToAdd", out coinsFromArgs))
                coinsFromArgs = RankSystemConfig.DEFAULT_COINS_TO_ADD;
            user.Coins += coinsFromArgs;
            DatabaseManager.UpsertUser(user);

            // Обновляем дневную статистику
            string today = DateTime.Now.ToString("yyyy-MM-dd");
            DatabaseManager.AddToDailyStatsInternal(user.Service, user.ServiceUserId, today, coins: coinsFromArgs);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] AddCoins Error: {ex}");
            return false;
        }
    }

    public bool GetCoins()
    {
        try
        {
            string targetUser = RankSystemInternal.NormalizeTargetUser(args["rawInput"].ToString());
            if (targetUser.Equals(""))
            {
                long coins = RankSystemInternal.GetCoins(this);
                CPH.SetArgument("coins", coins);
            }
            else
            {
                long coins = RankSystemInternal.GetCoins(this, targetUser);
                CPH.SetArgument("coins", coins);
                CPH.SetArgument("userName", targetUser);
            }

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetCoins Error: {ex}");
            return false;
        }
    }

    public bool CheckCoinsForAction()
    {
        try
        {
            if (args["eventSource"].ToString().Equals("command"))
            {
                if (!CPH.TryGetArg("userCoins", out long userCoins))
                {
                    userCoins = RankSystemInternal.GetCoins(this);
                }
                CPH.SetArgument("userCoins", userCoins);
                long actionCurrency = long.Parse(args["actionCurrency"].ToString());
                if (userCoins < actionCurrency)
                {
                    SendReply();
                    return false;
                }
                else
                {
                    CPH.SetArgument("coinsToAdd", -actionCurrency);
                    AddCoins();

                    // Обновляем дневную статистику для потраченных монет
                    string today = DateTime.Now.ToString("yyyy-MM-dd");
                    string service = RankSystemInternal.NormalizeService(this);
                    var user = RankSystemInternal.CreateUserFromArgs(this, service);
                    DatabaseManager.AddToDailyStatsInternal(user.Service, user.ServiceUserId, today, spentCoins: actionCurrency);

                    return true;
                }
            }
            else
            {
                return true;
            }
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetCoins Error: {ex}");
            return false;
        }
    }

    public bool ClearUsersCoins()
    {
        int rowsAffected = DatabaseManager.ClearAllUsersCoins();
        CPH.LogInfo($"[RankSystem] Cleared coins for {rowsAffected} users");
        return true;
    }

    public bool GetTopViewers()
    {
        try
        {
            if (!CPH.TryGetArg("topType", out string topType))
            {
                topType = "watchtime";
            }

            if (!CPH.TryGetArg("topCount", out int topCount))
            {
                topCount = RankSystemConfig.DEFAULT_TOP_COUNT;
            }

            var (fieldName, displayName) = topType switch
            {
                "watchtime" => ("WatchTime", "времени просмотра"),
                "messagecount" => ("MessageCount", "количеству сообщений"),
                "coins" => ("Coins", "монетам"),
                _ => throw new ArgumentException("Неизвестный тип топа")
            };
            var topUsers = DatabaseManager.GetTopUsers(fieldName, topCount);
            if (topUsers.Count == 0)
            {
                CPH.SetArgument("reply", "Топ пуст. Никто ещё не набрал статистики.");
                return true;
            }

            var topEntries = topUsers.Select((u, i) => $"{i + 1}. {u.UserName} ({u.Service}) - {RankSystemInternal.FormatValue(u, fieldName)}").ToList();
            CPH.SetArgument("reply", $"Топ по {displayName}: " + string.Join(", ", topEntries));
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetTopViewers Error: {ex}");
            return false;
        }
    }

    public bool SendReply()
    {
        string service = RankSystemInternal.NormalizeService(this);
        if (!CPH.TryGetArg("reply", out string reply))
        {
            reply = "Стример забыл настроить ответ на команду!";
        }

        if (service.Equals("twitch"))
            CPH.SendMessage(reply);
        else if (service.Equals("trovo"))
            CPH.SendTrovoMessage(reply);
        else
        {
            CPH.SetArgument("message", reply);
            CPH.ExecuteMethod("MiniChat Method Collection", "SendMessageReply");
        }

        return true;
    }

    public bool TransferFromMiniChat()
    {
        try
        {
            // Обеспечиваем наличие инфраструктуры для фиксации миграций (создается только по вызову миграции)
            DatabaseManager.EnsureMigrationInfrastructure("MiniChat");

            string jsonContent = File.ReadAllText("Live.json");
            var liveData = JsonConvert.DeserializeObject<List<LiveData>>(jsonContent);

            if (liveData == null)
            {
                throw new Exception("Не удалось прочитать данные из Live.json");
            }

            foreach (var data in liveData)
            {
                if (data.Type != "Follow" || data.Service == "Boosty")
                    continue;

                // Получаем существующие данные пользователя
                var existingUser = RankSystemInternal.GetExistingUser(
                    data.Service == "Unknown" ? "vkvideolive" : data.Service.ToLower(),
                    data.UserID
                );

                // Если уже есть отметка миграции для этого пользователя, пропускаем
                if (existingUser != null && DatabaseManager.HasMigrationMark(existingUser.UUID, "MiniChat"))
                    continue;

                // Если пользователь существует и у него есть дата фоллоу, пропускаем
                if (existingUser != null && existingUser.FollowDate != DateTime.MinValue)
                    continue;

                // Создаем или обновляем данные пользователя
                var user = new UserData
                {
                    Service = data.Service == "Unknown" ? "vkvideolive" : data.Service.ToLower(),
                    ServiceUserId = data.UserID,
                    UserName = existingUser?.UserName ?? data.UserName.ToLower(), // Используем существующий username если есть
                    FollowDate = DateTime.Parse(data.Date),
                    GameWhenFollow = null
                };

                DatabaseManager.UpsertUser(user);

                // Обновляем UUID после вставки (на случай нового пользователя)
                var refreshed = RankSystemInternal.GetExistingUser(user.Service, user.ServiceUserId);
                if (refreshed != null && !string.IsNullOrEmpty(refreshed.UUID))
                {
                    DatabaseManager.MarkMigration(refreshed.UUID, "MiniChat");
                }
            }

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] TransferFromMiniChat Error: {ex}");
            return false;
        }
    }

    public bool OpenRankSystemEditor()
    {
        Application.EnableVisualStyles();
        Application.SetCompatibleTextRenderingDefault(false);
        var form = new RankSystemForm();
        form.ShowDialog();
        return true;
    }
    public bool GetDailyStats()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = RankSystemInternal.CreateUserFromArgs(this, service);

            string date = DateTime.Now.ToString("yyyy-MM-dd");

            var dailyStats = DatabaseManager.GetDailyStatsForUser(user.Service, user.ServiceUserId, date);

            if (dailyStats != null)
            {
                CPH.SetArgument("dailyWatchTime", dailyStats.WatchTime);
                CPH.SetArgument("dailyMessageCount", dailyStats.MessageCount);
                CPH.SetArgument("dailyCoins", dailyStats.Coins);
                CPH.SetArgument("dailySpentCoins", dailyStats.SpentCoins);
                CPH.SetArgument("dailyDate", dailyStats.Date);
            }
            else
            {
                CPH.SetArgument("dailyWatchTime", 0);
                CPH.SetArgument("dailyMessageCount", 0);
                CPH.SetArgument("dailyCoins", 0);
                CPH.SetArgument("dailySpentCoins", 0);
                CPH.SetArgument("dailyDate", date);
            }

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetDailyStats Error: {ex}");
            return false;
        }
    }

    public bool GetWeeklyStats()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = RankSystemInternal.CreateUserFromArgs(this, service);

            var endDate = DateTime.Now;
            var startDate = endDate.AddDays(-6); // 7 дней включая сегодня

            var weeklyStats = DatabaseManager.GetDailyStatsForPeriod(user.Service, user.ServiceUserId, startDate, endDate);

            long totalWatchTime = 0;
            long totalMessageCount = 0;
            long totalCoins = 0;
            long totalSpentCoins = 0;

            foreach (var stat in weeklyStats)
            {
                totalWatchTime += stat.WatchTime;
                totalMessageCount += stat.MessageCount;
                totalCoins += stat.Coins;
                totalSpentCoins += stat.SpentCoins;
            }

            CPH.SetArgument("weeklyWatchTime", totalWatchTime);
            CPH.SetArgument("weeklyMessageCount", totalMessageCount);
            CPH.SetArgument("weeklyCoins", totalCoins);
            CPH.SetArgument("weeklySpentCoins", totalSpentCoins);
            CPH.SetArgument("weeklyStartDate", startDate.ToString("yyyy-MM-dd"));
            CPH.SetArgument("weeklyEndDate", endDate.ToString("yyyy-MM-dd"));

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetWeeklyStats Error: {ex}");
            return false;
        }
    }

    public bool GetCalendarWeeklyStats()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = RankSystemInternal.CreateUserFromArgs(this, service);

            var currentDate = DateTime.Now;

            // Находим начало календарной недели (понедельник)
            var startDate = currentDate.AddDays(-(int)currentDate.DayOfWeek + (int)DayOfWeek.Monday);
            if (currentDate.DayOfWeek == DayOfWeek.Sunday)
            {
                startDate = startDate.AddDays(-7); // Если сегодня воскресенье, берем предыдущую неделю
            }

            // Находим конец календарной недели (воскресенье)
            var endDate = startDate.AddDays(6);

            var calendarWeeklyStats = DatabaseManager.GetDailyStatsForPeriod(user.Service, user.ServiceUserId, startDate, endDate);

            long totalWatchTime = 0;
            long totalMessageCount = 0;
            long totalCoins = 0;
            long totalSpentCoins = 0;

            foreach (var stat in calendarWeeklyStats)
            {
                totalWatchTime += stat.WatchTime;
                totalMessageCount += stat.MessageCount;
                totalCoins += stat.Coins;
                totalSpentCoins += stat.SpentCoins;
            }

            CPH.SetArgument("calendarWeeklyWatchTime", totalWatchTime);
            CPH.SetArgument("calendarWeeklyMessageCount", totalMessageCount);
            CPH.SetArgument("calendarWeeklyCoins", totalCoins);
            CPH.SetArgument("calendarWeeklySpentCoins", totalSpentCoins);
            CPH.SetArgument("calendarWeeklyStartDate", startDate.ToString("yyyy-MM-dd"));
            CPH.SetArgument("calendarWeeklyEndDate", endDate.ToString("yyyy-MM-dd"));

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetCalendarWeeklyStats Error: {ex}");
            return false;
        }
    }

    public bool GetCalendarMonthlyStats()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = RankSystemInternal.CreateUserFromArgs(this, service);

            var endDate = DateTime.Now;
            var startDate = new DateTime(endDate.Year, endDate.Month, 1); // Первый день текущего месяца

            var monthlyStats = DatabaseManager.GetDailyStatsForPeriod(user.Service, user.ServiceUserId, startDate, endDate);

            long totalWatchTime = 0;
            long totalMessageCount = 0;
            long totalCoins = 0;
            long totalSpentCoins = 0;

            foreach (var stat in monthlyStats)
            {
                totalWatchTime += stat.WatchTime;
                totalMessageCount += stat.MessageCount;
                totalCoins += stat.Coins;
                totalSpentCoins += stat.SpentCoins;
            }

            CPH.SetArgument("monthlyWatchTime", totalWatchTime);
            CPH.SetArgument("monthlyMessageCount", totalMessageCount);
            CPH.SetArgument("monthlyCoins", totalCoins);
            CPH.SetArgument("monthlySpentCoins", totalSpentCoins);
            CPH.SetArgument("monthlyStartDate", startDate.ToString("yyyy-MM-dd"));
            CPH.SetArgument("monthlyEndDate", endDate.ToString("yyyy-MM-dd"));

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetMonthlyStats Error: {ex}");
            return false;
        }
    }

    public bool GetLast30DaysStats()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = RankSystemInternal.CreateUserFromArgs(this, service);

            var endDate = DateTime.Now;
            var startDate = endDate.AddDays(-29); // 30 дней включая сегодня

            var last30DaysStats = DatabaseManager.GetDailyStatsForPeriod(user.Service, user.ServiceUserId, startDate, endDate);

            long totalWatchTime = 0;
            long totalMessageCount = 0;
            long totalCoins = 0;
            long totalSpentCoins = 0;

            foreach (var stat in last30DaysStats)
            {
                totalWatchTime += stat.WatchTime;
                totalMessageCount += stat.MessageCount;
                totalCoins += stat.Coins;
                totalSpentCoins += stat.SpentCoins;
            }

            CPH.SetArgument("last30DaysWatchTime", totalWatchTime);
            CPH.SetArgument("last30DaysMessageCount", totalMessageCount);
            CPH.SetArgument("last30DaysCoins", totalCoins);
            CPH.SetArgument("last30DaysSpentCoins", totalSpentCoins);
            CPH.SetArgument("last30DaysStartDate", startDate.ToString("yyyy-MM-dd"));
            CPH.SetArgument("last30DaysEndDate", endDate.ToString("yyyy-MM-dd"));

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetLast30DaysStats Error: {ex}");
            return false;
        }
    }

    public bool GetCalendarYearlyStats()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = RankSystemInternal.CreateUserFromArgs(this, service);

            var endDate = DateTime.Now;
            var startDate = new DateTime(endDate.Year, 1, 1); // Первый день текущего года

            var yearlyStats = DatabaseManager.GetDailyStatsForPeriod(user.Service, user.ServiceUserId, startDate, endDate);

            long totalWatchTime = 0;
            long totalMessageCount = 0;
            long totalCoins = 0;
            long totalSpentCoins = 0;

            foreach (var stat in yearlyStats)
            {
                totalWatchTime += stat.WatchTime;
                totalMessageCount += stat.MessageCount;
                totalCoins += stat.Coins;
                totalSpentCoins += stat.SpentCoins;
            }

            CPH.SetArgument("yearlyWatchTime", totalWatchTime);
            CPH.SetArgument("yearlyMessageCount", totalMessageCount);
            CPH.SetArgument("yearlyCoins", totalCoins);
            CPH.SetArgument("yearlySpentCoins", totalSpentCoins);
            CPH.SetArgument("yearlyStartDate", startDate.ToString("yyyy-MM-dd"));
            CPH.SetArgument("yearlyEndDate", endDate.ToString("yyyy-MM-dd"));

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetYearlyStats Error: {ex}");
            return false;
        }
    }

    public bool GetLast365DaysStats()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = RankSystemInternal.CreateUserFromArgs(this, service);

            var endDate = DateTime.Now;
            var startDate = endDate.AddDays(-364); // 365 дней включая сегодня

            var last365DaysStats = DatabaseManager.GetDailyStatsForPeriod(user.Service, user.ServiceUserId, startDate, endDate);

            long totalWatchTime = 0;
            long totalMessageCount = 0;
            long totalCoins = 0;
            long totalSpentCoins = 0;

            foreach (var stat in last365DaysStats)
            {
                totalWatchTime += stat.WatchTime;
                totalMessageCount += stat.MessageCount;
                totalCoins += stat.Coins;
                totalSpentCoins += stat.SpentCoins;
            }

            CPH.SetArgument("last365DaysWatchTime", totalWatchTime);
            CPH.SetArgument("last365DaysMessageCount", totalMessageCount);
            CPH.SetArgument("last365DaysCoins", totalCoins);
            CPH.SetArgument("last365DaysSpentCoins", totalSpentCoins);
            CPH.SetArgument("last365DaysStartDate", startDate.ToString("yyyy-MM-dd"));
            CPH.SetArgument("last365DaysEndDate", endDate.ToString("yyyy-MM-dd"));

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetLast365DaysStats Error: {ex}");
            return false;
        }
    }

    public bool MigrateFromRutony()
    {
        try
        {
            // Обеспечиваем наличие инфраструктуры для фиксации миграций (создается только по вызову миграции)
            DatabaseManager.EnsureMigrationInfrastructure("RutonyChat");
            string service = RankSystemInternal.NormalizeService(this);
            string ranksDbPath = "ranks.db";

            if (!File.Exists(ranksDbPath))
            {
                CPH.LogError($"[RankSystem] Файл базы данных {ranksDbPath} не найден!");
                return false;
            }

            // Получаем данные текущего пользователя из команды
            var user = RankSystemInternal.CreateUserFromArgs(this, service);
            if (string.IsNullOrEmpty(user.UserName))
            {
                CPH.LogError($"[RankSystem] Не удалось получить имя пользователя из команды");
                return false;
            }

            CPH.LogInfo($"[RankSystem] Ищем данные для пользователя {user.UserName} в {ranksDbPath}...");

            // Ищем пользователя в базе ranks.db по нику
            using (var sourceConnection = new SQLiteConnection($"Data Source={ranksDbPath};Version=3;"))
            {
                sourceConnection.Open();

                using (var cmd = new SQLiteCommand("SELECT CreditsQty, TimeQty FROM ChatterRank WHERE Nickname = @Nickname", sourceConnection))
                {
                    cmd.Parameters.AddWithValue("@Nickname", user.UserName.ToLower());

                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            var creditsQty = Convert.ToInt64(reader["CreditsQty"] ?? 0);
                            var timeQty = Convert.ToInt64(reader["TimeQty"] ?? 0);

                            // Получаем существующего пользователя из нашей базы
                            var existingUser = RankSystemInternal.GetExistingUser(user.Service, user.ServiceUserId);

                            // Если уже есть отметка миграции для этого пользователя, пропускаем
                            if (existingUser != null && DatabaseManager.HasMigrationMark(existingUser.UUID, "RutonyChat"))
                            {
                                CPH.LogInfo($"[RankSystem] Пропуск: уже мигрирован {user.UserName} из RutonyChat");
                                return true;
                            }

                            if (existingUser != null)
                            {
                                // Обновляем существующего пользователя
                                existingUser.Coins += creditsQty;
                                existingUser.WatchTime += timeQty;
                                DatabaseManager.UpsertUser(existingUser);
                                // Отмечаем успешную миграцию
                                var refreshed = RankSystemInternal.GetExistingUser(existingUser.Service, existingUser.ServiceUserId);
                                if (refreshed != null && !string.IsNullOrEmpty(refreshed.UUID))
                                    DatabaseManager.MarkMigration(refreshed.UUID, "RutonyChat");
                                CPH.LogInfo($"[RankSystem] Обновлен пользователь {user.UserName}: +{creditsQty} монет, +{timeQty} времени просмотра");
                            }
                            else
                            {
                                // Создаем нового пользователя
                                user.Coins = creditsQty;
                                user.WatchTime = timeQty;
                                user.MessageCount = 0;
                                user.FollowDate = DateTime.MinValue;
                                user.GameWhenFollow = "";
                                DatabaseManager.UpsertUser(user);
                                // Отмечаем успешную миграцию
                                var refreshedNew = RankSystemInternal.GetExistingUser(user.Service, user.ServiceUserId);
                                if (refreshedNew != null && !string.IsNullOrEmpty(refreshedNew.UUID))
                                    DatabaseManager.MarkMigration(refreshedNew.UUID, "RutonyChat");
                                CPH.LogInfo($"[RankSystem] Создан пользователь {user.UserName}: {creditsQty} монет, {timeQty} времени просмотра");
                            }

                            // Устанавливаем аргументы для использования в других действиях
                            CPH.SetArgument("migratedCredits", creditsQty);
                            CPH.SetArgument("migratedTime", timeQty);
                            CPH.SetArgument("migratedUserName", user.UserName);

                            return true;
                        }
                        else
                        {
                            CPH.LogWarn($"[RankSystem] Пользователь {user.UserName} не найден в базе {ranksDbPath}");
                            return false;
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] MigrateFromRutony Error: {ex}");
            return false;
        }
    }
}

// ============================================================================
// ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ И УТИЛИТЫ
// ============================================================================

// Вспомогательные методы для работы с системой рангов

// ============================================================================
// ВНУТРЕННЯЯ ЛОГИКА СИСТЕМЫ РАНГОВ
// ============================================================================

// Внутренние методы для работы с системой рангов
public static class RankSystemInternal
{
    // Нормализация имени пользователя из rawInput
    public static string NormalizeTargetUser(string raw)
    {
        if (string.IsNullOrEmpty(raw))
            return string.Empty;
        string target = raw.ToLower().Trim();
        if (target.EndsWith(","))
            target = target.Substring(0, target.Length - 1).TrimEnd();
        return target;
    }
    // Получение существующего пользователя из базы данных
    // service: Сервис пользователя
    // serviceUserId: ID пользователя в сервисе
    // Возвращает: Существующий пользователь или null
    public static UserData GetExistingUser(string service, string serviceUserId)
    {
        return DatabaseManager.GetUserData(
            filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
            parameters: new[] {
                new SQLiteParameter("@Service", service),
                new SQLiteParameter("@ServiceUserId", serviceUserId)
            }
        ).FirstOrDefault();
    }

    // Создание объекта пользователя из аргументов (единый метод для всего проекта)
    // cph: Экземпляр CPHInline для доступа к аргументам и логированию
    // service: Сервис (twitch, trovo, etc.)
    // userName: Имя пользователя (опционально)
    // serviceUserId: ID пользователя в сервисе (опционально)
    // Возвращает: Объект UserData
    public static UserData CreateUserFromArgs(CPHInline cph, string service, string userName = null, string serviceUserId = null)
    {
        if (string.IsNullOrEmpty(serviceUserId))
        {
            if (!cph.CPH.TryGetArg("userId", out serviceUserId))
            {
                cph.CPH.TryGetArg("minichat.Data.UserID", out serviceUserId);
            }
        }

        // Если serviceUserId все еще null или пустая строка, создаем временный ID
        if (string.IsNullOrEmpty(serviceUserId))
        {
            cph.CPH.LogWarn($"[RankSystem] ServiceUserId is NULL or empty for service {service}. Using temporary ID.");
            serviceUserId = $"temp_{(string.IsNullOrEmpty(userName) ? DateTime.Now.Ticks.ToString() : userName)}";
        }

        // Если UserName не передан, берем из аргументов
        if (string.IsNullOrEmpty(userName))
        {
            if (cph.args.ContainsKey("userName"))
            {
                userName = cph.args["userName"].ToString().ToLower();
            }
            else if (cph.args.ContainsKey("users"))
            {
                try
                {
                    var usersList = cph.args["users"] as List<Dictionary<string, object>>;
                    if (usersList != null && usersList.Count > 0 && usersList[0].ContainsKey("userName"))
                    {
                        userName = usersList[0]["userName"].ToString().ToLower();
                    }
                }
                catch (Exception ex)
                {
                    cph.CPH.LogWarn($"[RankSystem] Failed to extract userName from users list: {ex.Message}");
                }
            }
        }

        // Если userName все еще null, используем временное имя
        if (string.IsNullOrEmpty(userName))
        {
            cph.CPH.LogWarn($"[RankSystem] UserName is NULL or empty for service {service}, userId {serviceUserId}. Using temporary name.");
            userName = $"user_{serviceUserId}";
        }

        return new UserData
        {
            Service = service,
            ServiceUserId = serviceUserId,
            UserName = userName
        };
    }

    // Отправка ответа пользователю через соответствующий сервис
    // cph: Экземпляр CPHInline
    // service: Сервис для отправки
    // reply: Текст ответа
    // Возвращает: true если отправка успешна
    public static bool SendReplyToService(CPHInline cph, string service, string reply)
    {
        if (string.IsNullOrEmpty(reply))
        {
            reply = "Стример забыл настроить ответ на команду!";
        }

        try
        {
            if (service.Equals("twitch", StringComparison.OrdinalIgnoreCase))
                cph.CPH.SendMessage(reply);
            else if (service.Equals("trovo", StringComparison.OrdinalIgnoreCase))
                cph.CPH.SendTrovoMessage(reply);
            else
            {
                cph.CPH.SetArgument("message", reply);
                cph.CPH.ExecuteMethod("MiniChat Method Collection", "SendMessageReply");
            }
            return true;
        }
        catch (Exception ex)
        {
            cph.CPH.LogError($"[RankSystem] SendReplyToService Error: {ex}");
            return false;
        }
    }

    // Валидация данных пользователя
    // user: Объект пользователя для валидации
    // Возвращает: true если данные валидны
    public static bool ValidateUserData(UserData user)
    {
        if (user == null)
        {
            return false;
        }

        if (string.IsNullOrEmpty(user.Service))
        {
            return false;
        }

        if (string.IsNullOrEmpty(user.ServiceUserId))
        {
            return false;
        }

        if (string.IsNullOrEmpty(user.UserName))
        {
            return false;
        }

        return true;
    }
    // Получение количества монет пользователя
    // cph: Экземпляр CPHInline
    // targetUser: Имя пользователя для получения количества монет (опционально)
    // Возвращает: Количество монет
    public static long GetCoins(CPHInline cph, string targetUser = null)
    {
        var userData = GetUserDataFromDatabase(cph, targetUser);
        return userData?.Coins ?? 0;
    }

    // Получение времени просмотра пользователя
    // cph: Экземпляр CPHInline
    // targetUser: Имя пользователя для получения времени просмотра (опционально)
    // Возвращает: Время просмотра в секундах
    public static long GetWatchTime(CPHInline cph, string targetUser = null)
    {
        var userData = GetUserDataFromDatabase(cph, targetUser);
        return userData?.WatchTime ?? 0;
    }

    // Получение даты подписки пользователя
    // cph: Экземпляр CPHInline
    // targetUser: Имя пользователя для получения даты подписки (опционально)
    // Возвращает: Дата подписки или DateTime.MinValue если не подписан
    public static DateTime GetFollowDate(CPHInline cph, string targetUser = null)
    {
        var userData = GetUserDataFromDatabase(cph, targetUser);
        return userData?.FollowDate ?? DateTime.MinValue;
    }

    // Получение количества сообщений пользователя
    // cph: Экземпляр CPHInline
    // targetUser: Имя пользователя для получения количества сообщений (опционально)
    // Возвращает: Количество сообщений
    public static long GetMessageCount(CPHInline cph, string targetUser = null)
    {
        var userData = GetUserDataFromDatabase(cph, targetUser);
        return userData?.MessageCount ?? 0;
    }

    // Получение игры при подписке пользователя
    // cph: Экземпляр CPHInline
    // targetUser: Имя пользователя для получения игры при подписке (опционально)
    // Возвращает: Название игры или пустая строка
    public static string GetGameWhenFollow(CPHInline cph, string targetUser = null)
    {
        var userData = GetUserDataFromDatabase(cph, targetUser);
        return userData?.GameWhenFollow ?? string.Empty;
    }

    // Получение данных пользователя из базы данных
    // cph: Экземпляр CPHInline
    // targetUser: Имя пользователя для поиска (опционально)
    // Возвращает: Данные пользователя или null если не найден
    private static UserData GetUserDataFromDatabase(CPHInline cph, string targetUser = null)
    {
        string service = NormalizeService(cph);

        // Определяем параметры запроса в зависимости от входных данных
        string filter;
        SQLiteParameter[] parameters;

        if (string.IsNullOrEmpty(targetUser))
        {
            var user = CreateUserFromArgs(cph, service);
            filter = "Service = @Service AND ServiceUserId = @ServiceUserId";
            parameters = new[] {
                new SQLiteParameter("@Service", user.Service),
                new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
            };
        }
        else
        {
            var userName = targetUser.TrimStart('@');
            filter = "Service = @Service AND UserName = @UserName";
            parameters = new[] {
                new SQLiteParameter("@Service", service),
                new SQLiteParameter("@UserName", userName)
            };
        }

        return DatabaseManager.GetUserData(filter, parameters).FirstOrDefault();
    }

    // Нормализация названия сервиса
    // cph: Экземпляр CPHInline
    // Возвращает: Нормализованное название сервиса
    public static string NormalizeService(CPHInline cph)
    {
        if (!cph.CPH.TryGetArg("eventSource", out string service))
            if (!cph.CPH.TryGetArg("commandSource", out service))
                ;

        if (service.Equals("misc"))
        {
            if (cph.args.ContainsKey("timerId") && (cph.args["timerId"].ToString().Equals("1da45ce2-2383-4431-8b42-b4f3314d2d79") || cph.args["timerName"].ToString().ToLower().Equals("vkvideolive")))
            {
                return "vkvideolive";
            }
        }

        if (service.Equals("command"))
            service = cph.args["commandSource"].ToString();
        return service.Equals("vkplay", StringComparison.OrdinalIgnoreCase) ? "vkvideolive" : service.ToLower();
    }

    // Форматирование значения пользователя для отображения
    // user: Пользователь
    // field: Поле для форматирования
    // Возвращает: Отформатированная строка
    public static string FormatValue(UserData user, string field)
    {
        return field switch
        {
            "WatchTime" => FormatDateTime(user.WatchTime),
            "MessageCount" => $"{user.MessageCount} сообщ.",
            "Coins" => $"{user.Coins} монет",
            _ => "0"
        };
    }

    // Форматирование времени в читаемый вид
    // totalSeconds: Общее количество секунд
    // Возвращает: Отформатированная строка времени
    public static string FormatDateTime(long totalSeconds)
    {
        int years = 0;
        int months = 0;
        int days = (int)(totalSeconds / (60 * 60 * 24));
        int hours = (int)((totalSeconds % (60 * 60 * 24)) / (60 * 60));
        int minutes = (int)((totalSeconds % (60 * 60)) / 60);
        int seconds = (int)(totalSeconds % 60);

        if (days >= 365)
        {
            years = days / 365;
            days %= 365;
        }

        if (days >= 30)
        {
            months = days / 30;
            days %= 30;
        }

        string result = "";
        if (years > 0)
            result += $"{years.ToString()} {GetYearWord(years)} ";
        if (months > 0)
            result += $"{months.ToString()} {GetMonthWord(months)} ";
        if (days > 0)
            result += $"{days.ToString()} {GetDayWord(days)} ";
        if (hours > 0)
            result += $"{hours.ToString()} {GetHourWord(hours)} ";
        if (minutes > 0)
            result += $"{minutes.ToString()} {GetMinuteWord(minutes)} ";
        if (seconds > 0)
            result += $"{seconds.ToString()} {GetSecondWord(seconds)}";
        return result.Trim();
    }

    #region Вспомогательные методы для склонения слов

    private static string GetYearWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "год";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "года";
        return "лет";
    }

    private static string GetMonthWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "месяц";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "месяца";
        return "месяцев";
    }

    private static string GetDayWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "день";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "дня";
        return "дней";
    }

    private static string GetHourWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "час";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "часа";
        return "часов";
    }

    private static string GetMinuteWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "минуту";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "минуты";
        return "минут";
    }

    private static string GetSecondWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "секунду";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "секунды";
        return "секунд";
    }

    #endregion
}

// ============================================================================
// РАБОТА С БАЗОЙ ДАННЫХ
// ============================================================================

// Менеджер для работы с базой данных системы рангов
public static class DatabaseManager
{
    // Путь к файлу базы данных
    private static readonly string DbPath = RankSystemConfig.DB_PATH;
    private static readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
    private static readonly ReaderWriterLockSlim _historyLock = new ReaderWriterLockSlim();
    private static SQLiteConnection CreateConnection()
    {
        var connection = new SQLiteConnection($"Data Source={DbPath};Version=3;");
        return connection;
    }

    public static void InitializeDatabase()
    {
        _lock.EnterWriteLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(connection))
                {
                    cmd.CommandText = $@"
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                    PRAGMA busy_timeout = {RankSystemConfig.DB_TIMEOUT};
                    PRAGMA cache_size = {RankSystemConfig.DB_CACHE_SIZE};
                    PRAGMA temp_store = MEMORY;
                    PRAGMA wal_autocheckpoint = 1000;";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = @"
                    CREATE TABLE IF NOT EXISTS Users (
                        UUID TEXT NOT NULL,
                        Service TEXT NOT NULL,
                        ServiceUserId TEXT NOT NULL,
                        UserName TEXT NOT NULL,
                        WatchTime INTEGER DEFAULT 0,
                        FollowDate TEXT NOT NULL,
                        MessageCount INTEGER DEFAULT 0,
                        Coins INTEGER DEFAULT 0,
                        GameWhenFollow TEXT,
                        PRIMARY KEY (Service, ServiceUserId)
                    );";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = @"
                    CREATE TABLE IF NOT EXISTS UserNameHistory (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        UUID TEXT NOT NULL,
                        OldUserName TEXT,
                        NewUserName TEXT,
                        ChangeDate TEXT NOT NULL,
                        FOREIGN KEY (UUID) REFERENCES Users(UUID)
                    );";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = @"
                    CREATE TABLE IF NOT EXISTS DailyStats (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        UUID TEXT,
                        Date TEXT NOT NULL,
                        ServiceUserId TEXT NOT NULL,
                        Service TEXT NOT NULL,
                        WatchTime INTEGER DEFAULT 0,
                        MessageCount INTEGER DEFAULT 0,
                        Coins INTEGER DEFAULT 0,
                        SpentCoins INTEGER DEFAULT 0,
                        UNIQUE(Date, ServiceUserId, Service)
                    );";
                    cmd.ExecuteNonQuery();

                    // Создаем индексы для таблицы DailyStats
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_dailystats_date ON DailyStats(Date);";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_dailystats_user ON DailyStats(ServiceUserId, Service);";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_dailystats_date_user ON DailyStats(Date, ServiceUserId, Service);";
                    cmd.ExecuteNonQuery();

                    // Check and add missing columns in Users table
                    var expectedColumns = new Dictionary<string, string>
                    {
                        { "UUID", "TEXT NOT NULL" },
                        { "Service", "TEXT NOT NULL" },
                        { "ServiceUserId", "TEXT NOT NULL" },
                        { "UserName", "TEXT NOT NULL" },
                        { "WatchTime", "INTEGER DEFAULT 0" },
                        { "FollowDate", "TEXT NOT NULL" },
                        { "MessageCount", "INTEGER DEFAULT 0" },
                        { "Coins", "INTEGER DEFAULT 0" },
                        { "GameWhenFollow", "TEXT" }
                    };

                    // Get existing columns
                    cmd.CommandText = "PRAGMA table_info(Users);";
                    var existingColumns = new HashSet<string>();
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            existingColumns.Add(reader["name"].ToString());
                        }
                    }

                    // Add missing columns
                    foreach (var column in expectedColumns)
                    {
                        if (!existingColumns.Contains(column.Key))
                        {
                            cmd.CommandText = $"ALTER TABLE Users ADD COLUMN {column.Key} {column.Value};";
                            cmd.ExecuteNonQuery();
                        }
                    }

                    // Check and add missing columns in UserNameHistory table
                    expectedColumns = new Dictionary<string, string>
                    {
                        { "Id", "INTEGER PRIMARY KEY AUTOINCREMENT" },
                        { "UUID", "TEXT NOT NULL" },
                        { "OldUserName", "TEXT" },
                        { "NewUserName", "TEXT" },
                        { "ChangeDate", "TEXT NOT NULL" }
                    };

                    // Get existing columns
                    cmd.CommandText = "PRAGMA table_info(UserNameHistory);";
                    existingColumns.Clear();
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            existingColumns.Add(reader["name"].ToString());
                        }
                    }

                    // Add missing columns
                    foreach (var column in expectedColumns)
                    {
                        if (!existingColumns.Contains(column.Key))
                        {
                            cmd.CommandText = $"ALTER TABLE UserNameHistory ADD COLUMN {column.Key} {column.Value};";
                            cmd.ExecuteNonQuery();
                        }
                    }

                    // Check and add missing columns in DailyStats table
                    expectedColumns = new Dictionary<string, string>
                    {
                        { "Id", "INTEGER PRIMARY KEY AUTOINCREMENT" },
                        { "Date", "TEXT NOT NULL" },
                        { "ServiceUserId", "TEXT NOT NULL" },
                        { "Service", "TEXT NOT NULL" },
                        { "WatchTime", "INTEGER DEFAULT 0" },
                        { "MessageCount", "INTEGER DEFAULT 0" },
                        { "Coins", "INTEGER DEFAULT 0" },
                        { "SpentCoins", "INTEGER DEFAULT 0" }
                    };

                    // Get existing columns
                    cmd.CommandText = "PRAGMA table_info(DailyStats);";
                    existingColumns.Clear();
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            existingColumns.Add(reader["name"].ToString());
                        }
                    }

                    // Add missing columns
                    foreach (var column in expectedColumns)
                    {
                        if (!existingColumns.Contains(column.Key))
                        {
                            cmd.CommandText = $"ALTER TABLE DailyStats ADD COLUMN {column.Key} {column.Value};";
                            cmd.ExecuteNonQuery();
                        }
                    }

                    // Блок заполнения Username и пересоздания таблицы более не требуется
                }
            }

            // Применяем миграции
            MigrateDailyStatsAddUUID();
            MigrateDailyStatsDropUsername();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public static void DropDatabase()
    {
        _lock.EnterWriteLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(connection))
                {
                    cmd.CommandText = @"
                PRAGMA writable_schema = 1;
                DELETE FROM sqlite_master WHERE type IN ('table', 'index', 'trigger');
                PRAGMA writable_schema = 0;
                VACUUM;";
                    cmd.ExecuteNonQuery();
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        InitializeDatabase();
    }

    // Миграция: добавление колонки UUID в таблицу DailyStats
    private static void MigrateDailyStatsAddUUID()
    {
        using (var connection = CreateConnection())
        {
            connection.Open();
            using (var cmd = new SQLiteCommand(connection))
            {
                // Проверяем, существует ли колонка UUID в DailyStats
                cmd.CommandText = "PRAGMA table_info(DailyStats);";
                var reader = cmd.ExecuteReader();
                bool hasUUID = false;
                while (reader.Read())
                {
                    if (reader["name"].ToString() == "UUID")
                    {
                        hasUUID = true;
                        break;
                    }
                }
                reader.Close();

                // Если колонки нет, добавляем её
                if (!hasUUID)
                {
                    cmd.CommandText = "ALTER TABLE DailyStats ADD COLUMN UUID TEXT;";
                    cmd.ExecuteNonQuery();

                    // Заполняем UUID для существующих записей из таблицы Users
                    cmd.CommandText = @"
                    UPDATE DailyStats 
                    SET UUID = (
                        SELECT UUID 
                        FROM Users 
                        WHERE Users.Service = DailyStats.Service 
                        AND Users.ServiceUserId = DailyStats.ServiceUserId
                    )
                    WHERE UUID IS NULL;";
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }

    // Миграция: удаление колонки Username из таблицы DailyStats
    private static void MigrateDailyStatsDropUsername()
    {
        using (var connection = CreateConnection())
        {
            connection.Open();
            using (var cmd = new SQLiteCommand(connection))
            {
                // Проверяем, есть ли колонка Username в DailyStats
                cmd.CommandText = "PRAGMA table_info(DailyStats);";
                using (var reader = cmd.ExecuteReader())
                {
                    bool hasUsername = false;
                    while (reader.Read())
                    {
                        if (reader["name"].ToString() == "Username")
                        {
                            hasUsername = true;
                            break;
                        }
                    }
                    reader.Close();

                    if (!hasUsername)
                    {
                        return; // Нечего мигрировать
                    }
                }

                // Пересоздаем таблицу без Username
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        cmd.Transaction = transaction;

                        // Создаем новую таблицу без Username
                        cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS DailyStats_new (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    UUID TEXT,
    Date TEXT NOT NULL,
    ServiceUserId TEXT NOT NULL,
    Service TEXT NOT NULL,
    WatchTime INTEGER DEFAULT 0,
    MessageCount INTEGER DEFAULT 0,
    Coins INTEGER DEFAULT 0,
    SpentCoins INTEGER DEFAULT 0,
    UNIQUE(Date, ServiceUserId, Service)
);";
                        cmd.ExecuteNonQuery();

                        // Копируем данные без Username
                        cmd.CommandText = @"
INSERT INTO DailyStats_new (Id, UUID, Date, ServiceUserId, Service, WatchTime, MessageCount, Coins, SpentCoins)
SELECT Id, UUID, Date, ServiceUserId, Service, WatchTime, MessageCount, Coins, SpentCoins
FROM DailyStats;";
                        cmd.ExecuteNonQuery();

                        // Удаляем старую таблицу и переименовываем новую
                        cmd.CommandText = "DROP TABLE DailyStats;";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "ALTER TABLE DailyStats_new RENAME TO DailyStats;";
                        cmd.ExecuteNonQuery();

                        // Пересоздаем индексы
                        cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_dailystats_date ON DailyStats(Date);";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_dailystats_user ON DailyStats(ServiceUserId, Service);";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_dailystats_date_user ON DailyStats(Date, ServiceUserId, Service);";
                        cmd.ExecuteNonQuery();

                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }
        }
    }

    public static void UpsertUser(UserData user)
    {
        // Логируем входные данные для отладки
        System.Diagnostics.Debug.WriteLine($"[RankSystem] UpsertUser called for user: {user.UserName} ({user.Service}), GameWhenFollow: '{user.GameWhenFollow}'");

        // Определим, является ли это добавлением сообщения
        bool isMessageIncrement = false;
        long coinsToAdd = 0;
        string oldUserName = null;
        string uuid = null;

        // Получаем данные о пользователе ДО входа в блокировку записи
        var existingUser = GetUserData(filter: "Service = @Service AND ServiceUserId = @ServiceUserId", parameters: new[] { new SQLiteParameter("@Service", user.Service), new SQLiteParameter("@ServiceUserId", user.ServiceUserId) }).FirstOrDefault();

        // Расчет дельты для инкрементов
        if (existingUser != null)
        {
            if (user.MessageCount > existingUser.MessageCount)
            {
                isMessageIncrement = true;
            }

            coinsToAdd = user.Coins - existingUser.Coins;

            // Проверяем, изменился ли никнейм
            if (!string.Equals(existingUser.UserName, user.UserName, StringComparison.OrdinalIgnoreCase))
            {
                oldUserName = existingUser.UserName;
                uuid = existingUser.UUID;
            }
        }
        else
        {
            // Новый пользователь
            coinsToAdd = user.Coins;
        }

        _lock.EnterWriteLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        using (var cmd = new SQLiteCommand(connection))
                        {
                            cmd.Transaction = transaction;
                            if (existingUser != null)
                            {
                                // Обновление существующего пользователя - используем атомарные обновления
                                string updateQuery = @"
                                UPDATE Users 
                                SET UserName = @UserName,
                                    WatchTime = @WatchTime,
                                    Coins = Coins + @CoinsToAdd";
                                if (isMessageIncrement)
                                {
                                    updateQuery += ", MessageCount = MessageCount + 1";
                                }

                                // Обновляем FollowDate только если он больше минимального значения
                                if (user.FollowDate > DateTime.MinValue)
                                {
                                    updateQuery += ", FollowDate = @FollowDate";
                                }

                                // GameWhenFollow обновляем всегда, если он не null
                                if (!string.IsNullOrEmpty(user.GameWhenFollow))
                                {
                                    updateQuery += ", GameWhenFollow = @GameWhenFollow";
                                }
                                else
                                {
                                    // Если GameWhenFollow пустой, устанавливаем его в NULL
                                    updateQuery += ", GameWhenFollow = NULL";
                                }

                                updateQuery += " WHERE Service = @Service AND ServiceUserId = @ServiceUserId";
                                cmd.CommandText = updateQuery;

                                // Логируем SQL запрос для отладки
                                System.Diagnostics.Debug.WriteLine($"[RankSystem] Update query: {updateQuery}");
                                cmd.Parameters.AddWithValue("@Service", user.Service);
                                cmd.Parameters.AddWithValue("@ServiceUserId", user.ServiceUserId);
                                cmd.Parameters.AddWithValue("@UserName", user.UserName);
                                cmd.Parameters.AddWithValue("@WatchTime", user.WatchTime);
                                cmd.Parameters.AddWithValue("@CoinsToAdd", coinsToAdd);
                                // Добавляем параметр FollowDate только если он больше минимального значения
                                if (user.FollowDate > DateTime.MinValue)
                                {
                                    cmd.Parameters.AddWithValue("@FollowDate", user.FollowDate.ToString("o"));
                                }

                                // GameWhenFollow обновляем всегда, если он не null
                                if (!string.IsNullOrEmpty(user.GameWhenFollow))
                                {
                                    cmd.Parameters.AddWithValue("@GameWhenFollow", user.GameWhenFollow);
                                }

                                cmd.ExecuteNonQuery();
                            }
                            else
                            {
                                // Вставка нового пользователя
                                cmd.CommandText = @"
                                INSERT INTO Users 
                            (UUID, Service, ServiceUserId, UserName, WatchTime, FollowDate, MessageCount, Coins, GameWhenFollow)
                            VALUES (
                                @UUID, @Service, @ServiceUserId, @UserName, @WatchTime, @FollowDate, @MessageCount, @Coins, @GameWhenFollow
                            )";
                                if (string.IsNullOrEmpty(user.UUID))
                                    user.UUID = Guid.NewGuid().ToString();
                                cmd.Parameters.AddWithValue("@UUID", user.UUID);
                                cmd.Parameters.AddWithValue("@Service", user.Service);
                                cmd.Parameters.AddWithValue("@ServiceUserId", user.ServiceUserId);
                                cmd.Parameters.AddWithValue("@UserName", user.UserName);
                                cmd.Parameters.AddWithValue("@WatchTime", user.WatchTime);
                                cmd.Parameters.AddWithValue("@FollowDate", user.FollowDate.ToString("o"));
                                cmd.Parameters.AddWithValue("@MessageCount", user.MessageCount);
                                cmd.Parameters.AddWithValue("@Coins", user.Coins);
                                cmd.Parameters.AddWithValue("@GameWhenFollow", user.GameWhenFollow ?? (object)DBNull.Value);
                                cmd.ExecuteNonQuery();
                            }
                        }

                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        // Если изменился никнейм, добавляем запись в историю после завершения основной транзакции
        if (oldUserName != null && uuid != null && !string.IsNullOrEmpty(user.UserName))
        {
            // Дополнительная проверка: убеждаемся, что имена действительно разные
            if (!string.Equals(oldUserName, user.UserName, StringComparison.OrdinalIgnoreCase))
            {
                AddUserNameHistory(uuid, oldUserName, user.UserName);
            }
        }
    }

    // =========================
    // МИГРАЦИОННАЯ ТАБЛИЦА ДЛЯ ВНЕШНИХ СЕРВИСОВ
    // =========================
    private const string MigrationTableName = "ExternalMigrations";

    private static void EnsureMigrationTableExists(SQLiteConnection connection)
    {
        using (var cmd = new SQLiteCommand(connection))
        {
            cmd.CommandText = $@"CREATE TABLE IF NOT EXISTS {MigrationTableName} (
                UUID TEXT PRIMARY KEY
            );";
            cmd.ExecuteNonQuery();
        }
    }

    private static void EnsureMigrationColumnExists(SQLiteConnection connection, string columnName)
    {
        using (var cmd = new SQLiteCommand(connection))
        {
            cmd.CommandText = $"PRAGMA table_info({MigrationTableName});";
            var existing = new HashSet<string>();
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    existing.Add(reader["name"].ToString());
                }
            }
            if (!existing.Contains(columnName))
            {
                cmd.CommandText = $"ALTER TABLE {MigrationTableName} ADD COLUMN {columnName} INTEGER DEFAULT 0;";
                cmd.ExecuteNonQuery();
            }
        }
    }

    public static void EnsureMigrationInfrastructure(string columnName)
    {
        using (var connection = CreateConnection())
        {
            connection.Open();
            EnsureMigrationTableExists(connection);
            EnsureMigrationColumnExists(connection, columnName);
        }
    }

    public static bool HasMigrationMark(string uuid, string columnName)
    {
        if (string.IsNullOrEmpty(uuid)) return false;
        using (var connection = CreateConnection())
        {
            connection.Open();
            EnsureMigrationTableExists(connection);
            EnsureMigrationColumnExists(connection, columnName);
            using (var cmd = new SQLiteCommand(connection))
            {
                cmd.CommandText = $"SELECT {columnName} FROM {MigrationTableName} WHERE UUID = @UUID";
                cmd.Parameters.AddWithValue("@UUID", uuid);
                var result = cmd.ExecuteScalar();
                if (result == null || result == DBNull.Value) return false;
                return Convert.ToInt64(result) != 0;
            }
        }
    }

    public static void MarkMigration(string uuid, string columnName)
    {
        if (string.IsNullOrEmpty(uuid)) return;
        using (var connection = CreateConnection())
        {
            connection.Open();
            EnsureMigrationTableExists(connection);
            EnsureMigrationColumnExists(connection, columnName);
            using (var cmd = new SQLiteCommand(connection))
            {
                cmd.CommandText = $@"INSERT INTO {MigrationTableName} (UUID, {columnName}) VALUES (@UUID, 1)
                                     ON CONFLICT(UUID) DO UPDATE SET {columnName} = 1";
                cmd.Parameters.AddWithValue("@UUID", uuid);
                cmd.ExecuteNonQuery();
            }
        }
    }

    public static List<UserData> GetUserData(string filter = null, SQLiteParameter[] parameters = null)
    {
        _lock.EnterReadLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(connection))
                {
                    cmd.CommandText = "SELECT * FROM Users";
                    if (!string.IsNullOrEmpty(filter))
                        cmd.CommandText += " WHERE " + filter;
                    if (parameters != null)
                        cmd.Parameters.AddRange(parameters);
                    using (var reader = cmd.ExecuteReader())
                    {
                        var users = new List<UserData>();
                        while (reader.Read())
                        {
                            users.Add(new UserData { UUID = reader["UUID"].ToString(), Service = reader["Service"].ToString(), ServiceUserId = reader["ServiceUserId"].ToString(), UserName = reader["UserName"].ToString(), WatchTime = Convert.ToInt64(reader["WatchTime"]), FollowDate = DateTime.Parse(reader["FollowDate"].ToString()), MessageCount = Convert.ToInt64(reader["MessageCount"]), Coins = Convert.ToInt64(reader["Coins"]), GameWhenFollow = reader["GameWhenFollow"] as string });
                        }

                        return users;
                    }
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public static List<UserData> GetTopUsers(string topType, int limit)
    {
        string orderBy = topType.ToLower() switch
        {
            "watchtime" => "WatchTime DESC",
            "messagecount" => "MessageCount DESC",
            "coins" => "Coins DESC",
            _ => throw new ArgumentException("Invalid topType")
        };
        return GetUserData(filter: $"{topType} > 0 ORDER BY {orderBy} LIMIT @limit", parameters: new[] { new SQLiteParameter("@limit", limit) });
    }

    public static void AddUserNameHistory(string uuid, string oldUserName, string newUserName)
    {
        _historyLock.EnterWriteLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(connection))
                {
                    cmd.CommandText = @"
                    INSERT INTO UserNameHistory (UUID, OldUserName, NewUserName, ChangeDate)
                    VALUES (@UUID, @OldUserName, @NewUserName, @ChangeDate)";
                    cmd.Parameters.AddWithValue("@UUID", uuid);
                    cmd.Parameters.AddWithValue("@OldUserName", oldUserName);
                    cmd.Parameters.AddWithValue("@NewUserName", newUserName);
                    cmd.Parameters.AddWithValue("@ChangeDate", DateTime.UtcNow.ToString("o"));
                    cmd.ExecuteNonQuery();
                }
            }
        }
        finally
        {
            _historyLock.ExitWriteLock();
        }
    }

    public static List<UserNameHistory> GetUserNameHistory(string uuid)
    {
        _lock.EnterReadLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(connection))
                {
                    cmd.CommandText = @"
                    SELECT h.*, u.Service, u.ServiceUserId 
                    FROM UserNameHistory h
                    JOIN Users u ON h.UUID = u.UUID
                    WHERE h.UUID = @UUID 
                    ORDER BY h.ChangeDate DESC";

                    cmd.Parameters.AddWithValue("@UUID", uuid);

                    using (var reader = cmd.ExecuteReader())
                    {
                        var history = new List<UserNameHistory>();
                        while (reader.Read())
                        {
                            history.Add(new UserNameHistory
                            {
                                Id = Convert.ToInt64(reader["Id"]),
                                UUID = reader["UUID"].ToString(),
                                OldUserName = reader["OldUserName"].ToString(),
                                NewUserName = reader["NewUserName"].ToString(),
                                ChangeDate = DateTime.Parse(reader["ChangeDate"].ToString()),
                                Service = reader["Service"].ToString(),
                                ServiceUserId = reader["ServiceUserId"].ToString()
                            });
                        }
                        return history;
                    }
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public static void DeleteUser(UserData user)
    {
        _lock.EnterWriteLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(connection))
                {
                    cmd.CommandText = "DELETE FROM Users WHERE Service = @Service AND ServiceUserId = @ServiceUserId";
                    cmd.Parameters.AddWithValue("@Service", user.Service);
                    cmd.Parameters.AddWithValue("@ServiceUserId", user.ServiceUserId);
                    cmd.ExecuteNonQuery();
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public static int ClearAllUsersCoins()
    {
        _lock.EnterWriteLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    using (var cmd = new SQLiteCommand("UPDATE Users SET Coins = 0", connection))
                    {
                        cmd.Transaction = transaction;
                        int rowsAffected = cmd.ExecuteNonQuery();
                        transaction.Commit();
                        return rowsAffected;
                    }
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    // Методы для работы с ежедневной статистикой

    public static List<DailyStats> GetDailyStats(string filter = null, SQLiteParameter[] parameters = null)
    {
        _lock.EnterReadLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(connection))
                {
                    cmd.CommandText = "SELECT * FROM DailyStats";
                    if (!string.IsNullOrEmpty(filter))
                        cmd.CommandText += " WHERE " + filter;
                    if (parameters != null)
                        cmd.Parameters.AddRange(parameters);

                    using (var reader = cmd.ExecuteReader())
                    {
                        var dailyStats = new List<DailyStats>();
                        while (reader.Read())
                        {
                            dailyStats.Add(new DailyStats
                            {
                                Id = Convert.ToInt64(reader["Id"]),
                                UUID = reader["UUID"] != DBNull.Value ? reader["UUID"].ToString() : null,
                                Date = reader["Date"].ToString(),
                                ServiceUserId = reader["ServiceUserId"].ToString(),
                                Service = reader["Service"].ToString(),
                                WatchTime = Convert.ToInt64(reader["WatchTime"]),
                                MessageCount = Convert.ToInt64(reader["MessageCount"]),
                                Coins = Convert.ToInt64(reader["Coins"]),
                                SpentCoins = Convert.ToInt64(reader["SpentCoins"])
                            });
                        }
                        return dailyStats;
                    }
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public static DailyStats GetDailyStatsForUser(string service, string serviceUserId, string date)
    {
        var result = GetDailyStats(
            filter: "Service = @Service AND ServiceUserId = @ServiceUserId AND Date = @Date",
            parameters: new[]
            {
                new SQLiteParameter("@Service", service),
                new SQLiteParameter("@ServiceUserId", serviceUserId),
                new SQLiteParameter("@Date", date)
            }
        );
        return result.FirstOrDefault();
    }

    public static List<DailyStats> GetDailyStatsForPeriod(string service, string serviceUserId, DateTime startDate, DateTime endDate)
    {
        return GetDailyStats(
            filter: "Service = @Service AND ServiceUserId = @ServiceUserId AND Date BETWEEN @StartDate AND @EndDate ORDER BY Date",
            parameters: new[]
            {
                new SQLiteParameter("@Service", service),
                new SQLiteParameter("@ServiceUserId", serviceUserId),
                new SQLiteParameter("@StartDate", startDate.ToString("yyyy-MM-dd")),
                new SQLiteParameter("@EndDate", endDate.ToString("yyyy-MM-dd"))
            }
        );
    }

    public static string GetLastDailyStatsDate(string service, string serviceUserId)
    {
        _lock.EnterReadLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(connection))
                {
                    cmd.CommandText = @"
                    SELECT MAX(Date) as LastDate FROM DailyStats 
                    WHERE Service = @Service AND ServiceUserId = @ServiceUserId";

                    cmd.Parameters.AddWithValue("@Service", service);
                    cmd.Parameters.AddWithValue("@ServiceUserId", serviceUserId);

                    var result = cmd.ExecuteScalar();
                    return result?.ToString();
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    // Методы для точечного обновления дневной статистики

    public static void AddToDailyStatsInternal(string service, string serviceUserId, string date, long watchTime = 0, long messageCount = 0, long coins = 0, long spentCoins = 0)
    {
        _lock.EnterWriteLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(connection))
                {
                    cmd.CommandText = @"
                    UPDATE DailyStats 
                    SET WatchTime = WatchTime + @WatchTime,
                        MessageCount = MessageCount + @MessageCount,
                        Coins = Coins + @Coins,
                        SpentCoins = SpentCoins + @SpentCoins
                    WHERE Date = @Date AND ServiceUserId = @ServiceUserId AND Service = @Service";

                    cmd.Parameters.AddWithValue("@Date", date);
                    cmd.Parameters.AddWithValue("@ServiceUserId", serviceUserId);
                    cmd.Parameters.AddWithValue("@Service", service);
                    cmd.Parameters.AddWithValue("@WatchTime", watchTime);
                    cmd.Parameters.AddWithValue("@MessageCount", messageCount);
                    cmd.Parameters.AddWithValue("@Coins", coins);
                    cmd.Parameters.AddWithValue("@SpentCoins", spentCoins);

                    int rowsAffected = cmd.ExecuteNonQuery();

                    // Если запись не найдена, создаем новую
                    if (rowsAffected == 0)
                    {
                        CreateDailyStatsInternal(service, serviceUserId, date, watchTime, messageCount, coins, spentCoins);
                    }
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public static void UpdateDailyStatsColumn(string service, string serviceUserId, string date, string column, long value)
    {
        _lock.EnterWriteLock();
        try
        {
            UpdateDailyStatsColumnInternal(service, serviceUserId, date, column, value);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    // Внутренний метод без блокировки для использования внутри уже заблокированных операций
    public static void UpdateDailyStatsColumnInternal(string service, string serviceUserId, string date, string column, long value)
    {
        using (var connection = CreateConnection())
        {
            connection.Open();
            using (var cmd = new SQLiteCommand(connection))
            {
                cmd.CommandText = $@"
                UPDATE DailyStats 
                SET {column} = @Value
                WHERE Date = @Date AND ServiceUserId = @ServiceUserId AND Service = @Service";

                cmd.Parameters.AddWithValue("@Date", date);
                cmd.Parameters.AddWithValue("@ServiceUserId", serviceUserId);
                cmd.Parameters.AddWithValue("@Service", service);
                cmd.Parameters.AddWithValue("@Value", value);

                int rowsAffected = cmd.ExecuteNonQuery();

                // Если запись не найдена, создаем новую с нулевыми значениями
                if (rowsAffected == 0)
                {
                    // Получаем UUID из таблицы Users
                    string uuid = null;
                    cmd.CommandText = "SELECT UUID FROM Users WHERE Service = @Service AND ServiceUserId = @ServiceUserId";
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@Service", service);
                    cmd.Parameters.AddWithValue("@ServiceUserId", serviceUserId);
                    var uuidObj = cmd.ExecuteScalar();
                    if (uuidObj != null && uuidObj != DBNull.Value)
                    {
                        uuid = uuidObj.ToString();
                    }

                    var dailyStats = new DailyStats
                    {
                        UUID = uuid,
                        Date = date,
                        ServiceUserId = serviceUserId,
                        Service = service,
                        WatchTime = 0,
                        MessageCount = 0,
                        Coins = 0,
                        SpentCoins = 0
                    };

                    // Устанавливаем значение для указанной колонки
                    switch (column.ToLower())
                    {
                        case "watchtime":
                            dailyStats.WatchTime = value;
                            break;
                        case "messagecount":
                            dailyStats.MessageCount = value;
                            break;
                        case "coins":
                            dailyStats.Coins = value;
                            break;
                        case "spentcoins":
                            dailyStats.SpentCoins = value;
                            break;
                    }

                    UpsertDailyStatsInternal(dailyStats);
                }
            }
        }
    }
    public static void CreateDailyStats(string service, string serviceUserId, string date, long watchTime = 0, long messageCount = 0, long coins = 0, long spentCoins = 0)
    {
        _lock.EnterWriteLock();
        try
        {
            CreateDailyStatsInternal(service, serviceUserId, date, watchTime, messageCount, coins, spentCoins);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    // Внутренний метод без блокировки для использования внутри уже заблокированных операций
    public static void CreateDailyStatsInternal(string service, string serviceUserId, string date, long watchTime = 0, long messageCount = 0, long coins = 0, long spentCoins = 0)
    {
        // Получаем UUID из таблицы Users
        string uuid = null;
        using (var connection = CreateConnection())
        {
            connection.Open();
            using (var cmd = new SQLiteCommand(connection))
            {
                cmd.CommandText = "SELECT UUID FROM Users WHERE Service = @Service AND ServiceUserId = @ServiceUserId";
                cmd.Parameters.AddWithValue("@Service", service);
                cmd.Parameters.AddWithValue("@ServiceUserId", serviceUserId);
                var result = cmd.ExecuteScalar();
                if (result != null)
                {
                    uuid = result.ToString();
                }
            }
        }

        var dailyStats = new DailyStats
        {
            UUID = uuid,
            Date = date,
            ServiceUserId = serviceUserId,
            Service = service,
            WatchTime = watchTime,
            MessageCount = messageCount,
            Coins = coins,
            SpentCoins = spentCoins
        };
        UpsertDailyStatsInternal(dailyStats);
    }

    public static void UpsertDailyStats(DailyStats dailyStats)
    {
        _lock.EnterWriteLock();
        try
        {
            UpsertDailyStatsInternal(dailyStats);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    // Внутренний метод без блокировки для использования внутри уже заблокированных операций
    public static void UpsertDailyStatsInternal(DailyStats dailyStats)
    {
        using (var connection = CreateConnection())
        {
            connection.Open();
            using (var cmd = new SQLiteCommand(connection))
            {
                cmd.CommandText = @"
                INSERT OR REPLACE INTO DailyStats (UUID, Date, ServiceUserId, Service, WatchTime, MessageCount, Coins, SpentCoins)
                VALUES (@UUID, @Date, @ServiceUserId, @Service, @WatchTime, @MessageCount, @Coins, @SpentCoins)";

                cmd.Parameters.AddWithValue("@UUID", dailyStats.UUID ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@Date", dailyStats.Date);
                cmd.Parameters.AddWithValue("@ServiceUserId", dailyStats.ServiceUserId);
                cmd.Parameters.AddWithValue("@Service", dailyStats.Service);
                cmd.Parameters.AddWithValue("@WatchTime", dailyStats.WatchTime);
                cmd.Parameters.AddWithValue("@MessageCount", dailyStats.MessageCount);
                cmd.Parameters.AddWithValue("@Coins", dailyStats.Coins);
                cmd.Parameters.AddWithValue("@SpentCoins", dailyStats.SpentCoins);

                cmd.ExecuteNonQuery();
            }
        }
    }
}

// ============================================================================
// UI КОМПОНЕНТЫ
// ============================================================================

// Форма для редактирования базы данных системы рангов
public class RankSystemForm : Form
{
    private DataGridView usersGrid = new DataGridView();
    private Button btnAdd = new Button();
    private Button btnSave = new Button();
    private Button btnRefresh = new Button();
    private Button btnDelete = new Button();
    private TextBox txtUUID = new TextBox();
    private TextBox txtService = new TextBox();
    private TextBox txtServiceUserId = new TextBox();
    private TextBox txtUserName = new TextBox();
    private TextBox txtWatchTime = new TextBox();
    private TextBox txtFollowDate = new TextBox();
    private TextBox txtMessageCount = new TextBox();
    private TextBox txtCoins = new TextBox();
    private TextBox txtGameWhenFollow = new TextBox();
    private Label lblUUID = new Label();
    private Label lblService = new Label();
    private Label lblServiceUserId = new Label();
    private Label lblUserName = new Label();
    private Label lblWatchTime = new Label();
    private Label lblFollowDate = new Label();
    private Label lblMessageCount = new Label();
    private Label lblCoins = new Label();
    private Label lblGameWhenFollow = new Label();
    private UserData selectedUser = null;
    internal List<UserData> allUsers = new List<UserData>();
    private BindingList<UserData> bindingUsers = new BindingList<UserData>();

    public RankSystemForm()
    {
        InitializeComponent();
        LoadUsers();
    }

    private void InitializeComponent()
    {
        this.Text = "RankSystem Database Editor";
        this.Size = new Size(1240, 600);
        this.MinimumSize = new Size(1240, 600);
        this.StartPosition = FormStartPosition.CenterScreen;

        // Настраиваем DataGridView
        usersGrid.Location = new Point(10, 10);
        usersGrid.Size = new Size(800, 480);
        usersGrid.SelectionMode = DataGridViewSelectionMode.FullRowSelect;
        usersGrid.MultiSelect = false;
        usersGrid.ReadOnly = true;
        usersGrid.AutoGenerateColumns = false;
        usersGrid.AllowUserToAddRows = false;
        usersGrid.AllowUserToDeleteRows = false;
        usersGrid.DataSource = null;
        usersGrid.CellClick += UsersGrid_CellClick;
        usersGrid.CellMouseClick += UsersGrid_CellMouseClick;
        usersGrid.ColumnHeaderMouseClick += UsersGrid_ColumnHeaderMouseClick;
        usersGrid.Anchor = AnchorStyles.Top | AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right;

        // Добавляем обработчики событий для обновления позиции фильтров
        usersGrid.ColumnWidthChanged += (s, e) => UpdateAllFilterPositions();
        usersGrid.Scroll += (s, e) => UpdateAllFilterPositions();
        usersGrid.SizeChanged += (s, e) => UpdateAllFilterPositions();
        this.Resize += (s, e) => UpdateAllFilterPositions();

        // Добавляем кастомные колонки с фильтрацией
        usersGrid.Columns.Add(new DataGridViewFilteredTextBoxColumn(this) { HeaderText = "UUID", DataPropertyName = "UUID", Width = 120, SortMode = DataGridViewColumnSortMode.Automatic });
        usersGrid.Columns.Add(new DataGridViewFilteredTextBoxColumn(this) { HeaderText = "Service", DataPropertyName = "Service", Width = 80, SortMode = DataGridViewColumnSortMode.Automatic });
        usersGrid.Columns.Add(new DataGridViewFilteredTextBoxColumn(this) { HeaderText = "ServiceUserId", DataPropertyName = "ServiceUserId", Width = 120, SortMode = DataGridViewColumnSortMode.Automatic });
        usersGrid.Columns.Add(new DataGridViewFilteredTextBoxColumn(this) { HeaderText = "UserName", DataPropertyName = "UserName", Width = 120, SortMode = DataGridViewColumnSortMode.Automatic });
        usersGrid.Columns.Add(new DataGridViewFilteredTextBoxColumn(this) { HeaderText = "WatchTime", DataPropertyName = "WatchTime", Width = 80, SortMode = DataGridViewColumnSortMode.Automatic });
        usersGrid.Columns.Add(new DataGridViewFilteredTextBoxColumn(this) { HeaderText = "FollowDate", DataPropertyName = "FollowDate", Width = 140, SortMode = DataGridViewColumnSortMode.Automatic });
        usersGrid.Columns.Add(new DataGridViewFilteredTextBoxColumn(this) { HeaderText = "MessageCount", DataPropertyName = "MessageCount", Width = 80, SortMode = DataGridViewColumnSortMode.Automatic });
        usersGrid.Columns.Add(new DataGridViewFilteredTextBoxColumn(this) { HeaderText = "Coins", DataPropertyName = "Coins", Width = 80, SortMode = DataGridViewColumnSortMode.Automatic });
        usersGrid.Columns.Add(new DataGridViewFilteredTextBoxColumn(this) { HeaderText = "GameWhenFollow", DataPropertyName = "GameWhenFollow", Width = 120, SortMode = DataGridViewColumnSortMode.Automatic });

        // Настраиваем правую панель с полями редактирования
        int left = 820, top = 40, spacing = 28, labelWidth = 110, boxWidth = 240;
        lblUUID.Text = "UUID:"; lblUUID.SetBounds(left, top, labelWidth, 20); lblUUID.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        txtUUID.SetBounds(left + labelWidth, top, boxWidth, 20); txtUUID.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        lblService.Text = "Service:"; lblService.SetBounds(left, top += spacing, labelWidth, 20); lblService.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        txtService.SetBounds(left + labelWidth, top, boxWidth, 20); txtService.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        lblServiceUserId.Text = "ServiceUserId:"; lblServiceUserId.SetBounds(left, top += spacing, labelWidth, 20); lblServiceUserId.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        txtServiceUserId.SetBounds(left + labelWidth, top, boxWidth, 20); txtServiceUserId.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        lblUserName.Text = "UserName:"; lblUserName.SetBounds(left, top += spacing, labelWidth, 20); lblUserName.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        txtUserName.SetBounds(left + labelWidth, top, boxWidth, 20); txtUserName.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        lblWatchTime.Text = "WatchTime:"; lblWatchTime.SetBounds(left, top += spacing, labelWidth, 20); lblWatchTime.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        txtWatchTime.SetBounds(left + labelWidth, top, boxWidth, 20); txtWatchTime.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        lblFollowDate.Text = "FollowDate (ISO):"; lblFollowDate.SetBounds(left, top += spacing, labelWidth, 20); lblFollowDate.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        txtFollowDate.SetBounds(left + labelWidth, top, boxWidth, 20); txtFollowDate.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        lblMessageCount.Text = "MessageCount:"; lblMessageCount.SetBounds(left, top += spacing, labelWidth, 20); lblMessageCount.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        txtMessageCount.SetBounds(left + labelWidth, top, boxWidth, 20); txtMessageCount.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        lblCoins.Text = "Coins:"; lblCoins.SetBounds(left, top += spacing, labelWidth, 20); lblCoins.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        txtCoins.SetBounds(left + labelWidth, top, boxWidth, 20); txtCoins.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        lblGameWhenFollow.Text = "GameWhenFollow:"; lblGameWhenFollow.SetBounds(left, top += spacing, labelWidth, 20); lblGameWhenFollow.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        txtGameWhenFollow.SetBounds(left + labelWidth, top, boxWidth, 20); txtGameWhenFollow.Anchor = AnchorStyles.Top | AnchorStyles.Right;

        btnAdd.Text = "Add New";
        btnAdd.SetBounds(left, top += spacing + 10, 80, 30); btnAdd.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        btnSave.Text = "Save";
        btnSave.SetBounds(left + 180, top, 80, 30); btnSave.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        btnRefresh.Text = "Refresh";
        btnRefresh.SetBounds(left, top + 40, 170, 30); btnRefresh.Anchor = AnchorStyles.Top | AnchorStyles.Right;
        btnDelete.Text = "Delete";
        btnDelete.SetBounds(left, top + 80, 170, 30); btnDelete.Anchor = AnchorStyles.Top | AnchorStyles.Right;

        this.Controls.Add(usersGrid);
        this.Controls.Add(lblUUID); this.Controls.Add(txtUUID);
        this.Controls.Add(lblService); this.Controls.Add(txtService);
        this.Controls.Add(lblServiceUserId); this.Controls.Add(txtServiceUserId);
        this.Controls.Add(lblUserName); this.Controls.Add(txtUserName);
        this.Controls.Add(lblWatchTime); this.Controls.Add(txtWatchTime);
        this.Controls.Add(lblFollowDate); this.Controls.Add(txtFollowDate);
        this.Controls.Add(lblMessageCount); this.Controls.Add(txtMessageCount);
        this.Controls.Add(lblCoins); this.Controls.Add(txtCoins);
        this.Controls.Add(lblGameWhenFollow); this.Controls.Add(txtGameWhenFollow);
        this.Controls.Add(btnAdd); this.Controls.Add(btnSave); this.Controls.Add(btnRefresh); this.Controls.Add(btnDelete);

        btnAdd.Click += BtnAdd_Click;
        btnSave.Click += BtnSave_Click;
        btnRefresh.Click += BtnRefresh_Click;
        btnDelete.Click += BtnDelete_Click;
    }

    private void LoadUsers()
    {
        // Загружаем данные из базы
        allUsers = DatabaseManager.GetUserData();

        // Создаем новый BindingList с загруженными данными
        bindingUsers = new BindingList<UserData>(allUsers);

        // Обновляем DataSource
        usersGrid.DataSource = bindingUsers;

        // Убеждаемся, что все записи отображаются
        usersGrid.Refresh();

        // Обновляем заголовок формы
        UpdateFormTitle();

        // Логируем количество загруженных записей для отладки
        System.Diagnostics.Debug.WriteLine($"[RankSystem] Loaded {allUsers.Count} users from database");
    }

    internal void UpdateFormTitle()
    {
        int totalCount = allUsers.Count;
        int filteredCount = GetFilteredCount();
        bool hasFilters = HasActiveFilters();

        string title = $"RankSystem Database Editor - {filteredCount} of {totalCount} users";
        if (hasFilters)
        {
            title += " (filtered)";
        }

        this.Text = title;
    }

    private void UsersGrid_CellClick(object sender, DataGridViewCellEventArgs e)
    {
        if (e.RowIndex >= 0)
        {
            var user = usersGrid.Rows[e.RowIndex].DataBoundItem as UserData;
            if (user != null)
            {
                selectedUser = user;
                FillUserFields(user);
            }
        }
    }

    private void UsersGrid_CellMouseClick(object sender, DataGridViewCellMouseEventArgs e)
    {
        // Обрабатываем клики по заголовкам (RowIndex = -1)
        if (e.RowIndex == -1 && e.ColumnIndex >= 0)
        {
            System.Diagnostics.Debug.WriteLine($"Header clicked: Column {e.ColumnIndex}, X={e.X}, Y={e.Y}");

            // Проверяем, была ли нажата кнопка фильтра
            var column = usersGrid.Columns[e.ColumnIndex];
            if (column is DataGridViewFilteredTextBoxColumn filteredColumn)
            {
                var cellBounds = usersGrid.GetCellDisplayRectangle(e.ColumnIndex, -1, true);
                var rightArea = cellBounds.Width - 35; // Правая область 35 пикселей (размер кнопки)

                if (e.X > rightArea)
                {
                    System.Diagnostics.Debug.WriteLine($"Filter button clicked via CellMouseClick for column {column.DataPropertyName}");
                    filteredColumn.ShowFilter();
                }
            }
        }
        // Обрабатываем клики по ячейкам данных
        else if (e.RowIndex >= 0)
        {
            var user = usersGrid.Rows[e.RowIndex].DataBoundItem as UserData;
            if (user != null)
            {
                selectedUser = user;
                FillUserFields(user);
            }
        }
    }

    private void UsersGrid_ColumnHeaderMouseClick(object sender, DataGridViewCellMouseEventArgs e)
    {
        // Обрабатываем клики по заголовкам колонок
        if (e.RowIndex == -1 && e.ColumnIndex >= 0)
        {
            System.Diagnostics.Debug.WriteLine($"Header clicked: Column {e.ColumnIndex}, X={e.X}, Y={e.Y}");

            // Проверяем, была ли нажата кнопка фильтра
            var column = usersGrid.Columns[e.ColumnIndex];
            if (column is DataGridViewFilteredTextBoxColumn filteredColumn)
            {
                var cellBounds = usersGrid.GetCellDisplayRectangle(e.ColumnIndex, -1, true);
                var rightArea = cellBounds.Width - 35; // Правая область 35 пикселей (размер кнопки)

                if (e.X > rightArea)
                {
                    System.Diagnostics.Debug.WriteLine($"Filter button clicked via ColumnHeaderMouseClick for column {column.DataPropertyName}");
                    filteredColumn.ShowFilter();
                }
            }
        }
    }

    private void FillUserFields(UserData user)
    {
        txtUUID.Text = user.UUID;
        txtService.Text = user.Service;
        txtServiceUserId.Text = user.ServiceUserId;
        txtUserName.Text = user.UserName;
        txtWatchTime.Text = user.WatchTime.ToString();
        txtFollowDate.Text = user.FollowDate.ToString("o");
        txtMessageCount.Text = user.MessageCount.ToString();
        txtCoins.Text = user.Coins.ToString();
        // Правильно обрабатываем null значения для GameWhenFollow
        txtGameWhenFollow.Text = user.GameWhenFollow ?? "";
    }

    private UserData GetUserFromFields()
    {
        // Обрабатываем GameWhenFollow специально - пустая строка должна стать null
        string gameWhenFollow = string.IsNullOrWhiteSpace(txtGameWhenFollow.Text) ? null : txtGameWhenFollow.Text.Trim();

        var user = new UserData
        {
            UUID = txtUUID.Text,
            Service = txtService.Text,
            ServiceUserId = txtServiceUserId.Text,
            UserName = txtUserName.Text,
            WatchTime = long.TryParse(txtWatchTime.Text, out var wt) ? wt : 0,
            FollowDate = DateTime.TryParse(txtFollowDate.Text, out var fd) ? fd : DateTime.MinValue,
            MessageCount = long.TryParse(txtMessageCount.Text, out var mc) ? mc : 0,
            Coins = long.TryParse(txtCoins.Text, out var c) ? c : 0,
            GameWhenFollow = gameWhenFollow
        };

        // Логируем созданный объект для отладки
        System.Diagnostics.Debug.WriteLine($"[RankSystem] GetUserFromFields created user: {user.UserName}, GameWhenFollow: '{user.GameWhenFollow}'");

        return user;
    }

    private void BtnAdd_Click(object sender, EventArgs e)
    {
        ClearUserFields();
        txtUUID.Text = Guid.NewGuid().ToString();
        selectedUser = null;
        usersGrid.ClearSelection();
    }

    private void BtnSave_Click(object sender, EventArgs e)
    {
        var user = GetUserFromFields();

        // Логируем данные перед сохранением для отладки
        System.Diagnostics.Debug.WriteLine($"[RankSystem] Saving user: {user.UserName} ({user.Service}), GameWhenFollow: '{user.GameWhenFollow}'");

        DatabaseManager.UpsertUser(user);

        // Обновляем данные и восстанавливаем полный список
        LoadUsers();
        ClearAllFilters();

        MessageBox.Show("User saved.");
    }

    private void BtnRefresh_Click(object sender, EventArgs e)
    {
        // Сначала очищаем все фильтры
        ClearAllFilters();

        // Затем загружаем данные заново
        LoadUsers();

        // Убеждаемся, что все данные отображаются
        if (usersGrid.DataSource is BindingList<UserData> bindingList)
        {
            // Проверяем, что количество отображаемых записей соответствует общему количеству
            if (bindingList.Count != allUsers.Count)
            {
                // Если количество не совпадает, принудительно обновляем DataSource
                usersGrid.DataSource = new BindingList<UserData>(allUsers);
            }
        }
    }

    private void BtnDelete_Click(object sender, EventArgs e)
    {
        if (selectedUser == null)
        {
            MessageBox.Show("Выберите пользователя для удаления.");
            return;
        }
        var result = MessageBox.Show($"Удалить пользователя {selectedUser.UserName} ({selectedUser.Service})?", "Подтверждение удаления", MessageBoxButtons.YesNo, MessageBoxIcon.Warning);
        if (result == DialogResult.Yes)
        {
            DatabaseManager.DeleteUser(selectedUser);

            // Обновляем данные и восстанавливаем полный список
            LoadUsers();
            ClearAllFilters();
            ClearUserFields();
            selectedUser = null;
        }
    }

    private void ClearUserFields()
    {
        txtUUID.Text = "";
        txtService.Text = "";
        txtServiceUserId.Text = "";
        txtUserName.Text = "";
        txtWatchTime.Text = "0";
        txtFollowDate.Text = "0001-01-01T00:00:00.0000000";
        txtMessageCount.Text = "0";
        txtCoins.Text = "0";
        txtGameWhenFollow.Text = ""; // Пустая строка для GameWhenFollow
    }

    private void ClearAllFilters()
    {
        // Очищаем фильтры во всех колонках
        foreach (DataGridViewColumn column in usersGrid.Columns)
        {
            if (column is DataGridViewFilteredTextBoxColumn filteredColumn)
            {
                filteredColumn.ClearFilter();
            }
        }

        // Восстанавливаем отображение всех данных
        if (usersGrid.DataSource is BindingList<UserData> currentBindingList)
        {
            // Если текущий список не содержит все данные, восстанавливаем его
            if (currentBindingList.Count != allUsers.Count)
            {
                usersGrid.DataSource = new BindingList<UserData>(allUsers);
                System.Diagnostics.Debug.WriteLine($"[RankSystem] Restored full data view: {allUsers.Count} users");
            }
        }

        // Обновляем отображение
        usersGrid.Refresh();

        // Обновляем заголовок формы
        UpdateFormTitle();
    }

    private void UpdateAllFilterPositions()
    {
        foreach (DataGridViewColumn column in usersGrid.Columns)
        {
            if (column is DataGridViewFilteredTextBoxColumn filteredColumn)
            {
                filteredColumn.UpdateFilterPosition();
            }
        }
    }

    private bool HasActiveFilters()
    {
        foreach (DataGridViewColumn column in usersGrid.Columns)
        {
            if (column is DataGridViewFilteredTextBoxColumn filteredColumn)
            {
                if (!string.IsNullOrEmpty(filteredColumn.GetFilterText()))
                    return true;
            }
        }
        return false;
    }

    private int GetFilteredCount()
    {
        if (usersGrid.DataSource is BindingList<UserData> bindingList)
        {
            return bindingList.Count;
        }
        return 0;
    }
}

// Кастомная колонка с фильтрацией в заголовке
public class DataGridViewFilteredTextBoxColumn : DataGridViewTextBoxColumn
{
    private TextBox filterTextBox;
    private bool isFilterVisible = false;
    private RankSystemForm parentForm;

    public DataGridViewFilteredTextBoxColumn(RankSystemForm form = null)
    {
        parentForm = form;
        // Создаем кастомную ячейку заголовка
        HeaderCell = new DataGridViewFilteredHeaderCell(this);
    }

    public void SetParentForm(RankSystemForm form)
    {
        parentForm = form;
        var headerCell = HeaderCell as DataGridViewFilteredHeaderCell;
        headerCell?.SetParentForm(form);
    }

    public void ShowFilter()
    {
        if (!isFilterVisible)
        {
            var headerCell = HeaderCell as DataGridViewFilteredHeaderCell;
            headerCell?.ShowFilter();
            isFilterVisible = true;
        }
    }

    public void HideFilter()
    {
        if (isFilterVisible)
        {
            var headerCell = HeaderCell as DataGridViewFilteredHeaderCell;
            headerCell?.HideFilter();
            isFilterVisible = false;
        }
    }

    public void ClearFilter()
    {
        var headerCell = HeaderCell as DataGridViewFilteredHeaderCell;
        headerCell?.ClearFilter();
    }

    public string GetFilterText()
    {
        var headerCell = HeaderCell as DataGridViewFilteredHeaderCell;
        return headerCell?.GetFilterText() ?? "";
    }

    public void UpdateFilterPosition()
    {
        var headerCell = HeaderCell as DataGridViewFilteredHeaderCell;
        headerCell?.UpdateFilterPosition();
    }
}

// Кастомная ячейка заголовка с фильтром
public class DataGridViewFilteredHeaderCell : DataGridViewColumnHeaderCell
{
    private TextBox filterTextBox;
    private Button filterButton;
    private Panel filterPanel;
    private DataGridViewFilteredTextBoxColumn parentColumn;
    private bool isFilterVisible = false;
    private RankSystemForm parentForm;

    public DataGridViewFilteredHeaderCell(DataGridViewFilteredTextBoxColumn column)
    {
        parentColumn = column;
        InitializeFilterControls();
    }

    public void SetParentForm(RankSystemForm form)
    {
        parentForm = form;
    }

    private void InitializeFilterControls()
    {
        // Создаем панель для фильтра
        filterPanel = new Panel();
        filterPanel.Visible = false;
        filterPanel.BackColor = Color.White;
        filterPanel.BorderStyle = BorderStyle.FixedSingle;
        filterPanel.LostFocus += FilterPanel_LostFocus;

        // Создаем кнопку фильтра
        filterButton = new Button();
        filterButton.Text = "🔍";
        filterButton.Size = new Size(20, 20);
        filterButton.Click += FilterButton_Click;
        filterButton.FlatStyle = FlatStyle.Flat;
        filterButton.BackColor = Color.Transparent;

        // Создаем текстовое поле фильтра
        filterTextBox = new TextBox();
        filterTextBox.Size = new Size(80, 20);
        filterTextBox.TextChanged += FilterTextBox_TextChanged;
        filterTextBox.KeyDown += FilterTextBox_KeyDown;
        filterTextBox.LostFocus += FilterTextBox_LostFocus;

        // Размещаем элементы
        filterPanel.Controls.Add(filterButton);
        filterPanel.Controls.Add(filterTextBox);
        filterButton.Location = new Point(0, 0);
        filterTextBox.Location = new Point(25, 0);
        filterPanel.Size = new Size(105, 22);

        // Добавляем панель к DataGridView
        if (DataGridView != null)
        {
            DataGridView.Controls.Add(filterPanel);
        }
    }

    protected override void OnDataGridViewChanged()
    {
        base.OnDataGridViewChanged();
        if (DataGridView != null)
        {
            DataGridView.Controls.Add(filterPanel);
            UpdateFilterPosition();
        }
    }

    // Обработка клика по заголовку
    protected override void OnMouseClick(DataGridViewCellMouseEventArgs e)
    {
        base.OnMouseClick(e);

        // Упрощенная логика: если клик в правой части заголовка, показываем фильтр
        var cellBounds = DataGridView.GetCellDisplayRectangle(ColumnIndex, -1, true);
        var rightArea = cellBounds.Width - 35; // Правая область 35 пикселей (размер кнопки)

        if (e.X > rightArea)
        {
            System.Diagnostics.Debug.WriteLine($"Filter button clicked for column {parentColumn.DataPropertyName}");
            ToggleFilter();
        }
    }

    // Добавляем обработку двойного клика
    protected override void OnMouseDoubleClick(DataGridViewCellMouseEventArgs e)
    {
        base.OnMouseDoubleClick(e);

        // Проверяем, был ли клик в области кнопки фильтра
        var cellBounds = DataGridView.GetCellDisplayRectangle(ColumnIndex, -1, true);
        var relativeX = e.X - (cellBounds.X + cellBounds.Width - 25);
        var relativeY = e.Y - (cellBounds.Y + 2);

        if (relativeX >= 0 && relativeX <= 20 && relativeY >= 0 && relativeY <= 20)
        {
            System.Diagnostics.Debug.WriteLine($"Filter button double-clicked for column {parentColumn.DataPropertyName}");
            ToggleFilter();
        }
    }

    private void FilterButton_Click(object sender, EventArgs e)
    {
        ToggleFilter();
    }

    private void ToggleFilter()
    {
        if (isFilterVisible)
        {
            HideFilter();
        }
        else
        {
            ShowFilter();
        }
    }

    public void ShowFilter()
    {
        filterPanel.Visible = true;
        UpdateFilterPosition();
        filterTextBox.Focus();
        isFilterVisible = true;

        // Обновляем отображение заголовка
        if (DataGridView != null)
        {
            DataGridView.InvalidateColumn(parentColumn.Index);
        }
    }

    public void HideFilter()
    {
        filterPanel.Visible = false;
        isFilterVisible = false;

        // Обновляем отображение заголовка
        if (DataGridView != null)
        {
            DataGridView.InvalidateColumn(parentColumn.Index);
        }
    }

    public void ClearFilter()
    {
        filterTextBox.Text = "";
        ApplyFilter("");

        // Обновляем отображение заголовка
        if (DataGridView != null)
        {
            DataGridView.InvalidateColumn(parentColumn.Index);
        }
    }

    public string GetFilterText()
    {
        return filterTextBox.Text;
    }

    private void FilterTextBox_TextChanged(object sender, EventArgs e)
    {
        ApplyFilter(filterTextBox.Text);

        // Обновляем отображение заголовка для показа индикации фильтра
        if (DataGridView != null)
        {
            DataGridView.InvalidateColumn(parentColumn.Index);
        }
    }

    private void FilterTextBox_KeyDown(object sender, KeyEventArgs e)
    {
        if (e.KeyCode == Keys.Escape)
        {
            HideFilter();
        }
    }

    private void FilterTextBox_LostFocus(object sender, EventArgs e)
    {
        // Простое закрытие фильтра при потере фокуса
        HideFilter();
    }

    private void FilterPanel_LostFocus(object sender, EventArgs e)
    {
        // Простое закрытие фильтра при потере фокуса
        HideFilter();
    }

    private void ApplyFilter(string filterText)
    {
        if (DataGridView?.DataSource is BindingList<UserData> bindingList)
        {
            // Получаем оригинальный список данных
            var originalList = parentForm?.allUsers ?? DatabaseManager.GetUserData();

            // Применяем фильтры от всех колонок
            var filteredList = originalList.Where(user =>
            {
                // Проверяем фильтр для текущей колонки
                var columnName = parentColumn.DataPropertyName;
                var property = typeof(UserData).GetProperty(columnName);
                if (property == null) return true;

                var value = property.GetValue(user);
                if (value == null) return false;

                if (string.IsNullOrEmpty(filterText)) return true;

                var valueString = value.ToString().ToLowerInvariant();
                return valueString.Contains(filterText.ToLowerInvariant());
            }).ToList();

            // Применяем фильтры от других колонок
            foreach (DataGridViewColumn column in DataGridView.Columns)
            {
                if (column is DataGridViewFilteredTextBoxColumn otherColumn && otherColumn != parentColumn)
                {
                    var otherFilterText = otherColumn.GetFilterText();
                    if (!string.IsNullOrEmpty(otherFilterText))
                    {
                        var otherColumnName = otherColumn.DataPropertyName;
                        var otherProperty = typeof(UserData).GetProperty(otherColumnName);
                        if (otherProperty != null)
                        {
                            filteredList = filteredList.Where(user =>
                            {
                                var value = otherProperty.GetValue(user);
                                if (value == null) return false;
                                var valueString = value.ToString().ToLowerInvariant();
                                return valueString.Contains(otherFilterText.ToLowerInvariant());
                            }).ToList();
                        }
                    }
                }
            }

            // Обновляем DataSource только если есть фильтры
            if (!string.IsNullOrEmpty(filterText) || HasAnyActiveFilters())
            {
                DataGridView.DataSource = new BindingList<UserData>(filteredList);
            }
            else
            {
                // Если нет активных фильтров, восстанавливаем полный список
                DataGridView.DataSource = new BindingList<UserData>(originalList);
            }

            // Логируем для отладки
            System.Diagnostics.Debug.WriteLine($"[RankSystem] Filter applied: {filteredList.Count} of {originalList.Count} users shown");

            // Обновляем заголовок формы
            if (parentForm != null)
            {
                parentForm.UpdateFormTitle();
            }
        }
    }

    private bool HasAnyActiveFilters()
    {
        if (DataGridView?.Columns == null) return false;

        foreach (DataGridViewColumn column in DataGridView.Columns)
        {
            if (column is DataGridViewFilteredTextBoxColumn filteredColumn)
            {
                if (!string.IsNullOrEmpty(filteredColumn.GetFilterText()))
                    return true;
            }
        }
        return false;
    }

    public void UpdateFilterPosition()
    {
        if (DataGridView != null && parentColumn != null)
        {
            var columnIndex = parentColumn.Index;
            var headerBounds = DataGridView.GetCellDisplayRectangle(columnIndex, -1, true);

            filterPanel.Location = new Point(
                headerBounds.X + headerBounds.Width - filterPanel.Width - 5,
                headerBounds.Y + headerBounds.Height - filterPanel.Height - 2
            );
        }
    }

    protected override void Paint(Graphics graphics, Rectangle clipBounds, Rectangle cellBounds, int rowIndex, DataGridViewElementStates cellState, object value, object formattedValue, string errorText, DataGridViewCellStyle cellStyle, DataGridViewAdvancedBorderStyle advancedBorderStyle, DataGridViewPaintParts paintParts)
    {
        base.Paint(graphics, clipBounds, cellBounds, rowIndex, cellState, value, formattedValue, errorText, cellStyle, advancedBorderStyle, paintParts);

        // Рисуем кнопку фильтра в заголовке (увеличиваем размер)
        var buttonRect = new Rectangle(cellBounds.Right - 35, cellBounds.Top + 2, 30, 20);

        // Рисуем фон кнопки (меняем цвет если фильтр активен)
        if (isFilterVisible && !string.IsNullOrEmpty(filterTextBox.Text))
        {
            graphics.FillRectangle(Brushes.LightGreen, buttonRect);
            graphics.DrawRectangle(Pens.DarkGreen, buttonRect);
        }
        else
        {
            graphics.FillRectangle(Brushes.LightBlue, buttonRect);
            graphics.DrawRectangle(Pens.DarkBlue, buttonRect);
        }

        // Рисуем иконку фильтра
        using (var font = new Font("Arial", 10))
        {
            graphics.DrawString("🔍", font, Brushes.Black, buttonRect, new StringFormat { Alignment = StringAlignment.Center, LineAlignment = StringAlignment.Center });
        }

        // Если фильтр активен и есть текст, показываем индикатор
        if (isFilterVisible && !string.IsNullOrEmpty(filterTextBox.Text))
        {
            // Рисуем маленький индикатор в левом верхнем углу кнопки
            var indicatorRect = new Rectangle(buttonRect.X + 2, buttonRect.Y + 2, 6, 6);
            graphics.FillEllipse(Brushes.Orange, indicatorRect);
            graphics.DrawEllipse(Pens.DarkOrange, indicatorRect);
        }
    }
}