///----------------------------------------------------------------------------
///   Module:       RankSystem
///   Author:       NuboHeimer (https://live.vkvideo.ru/nuboheimer)
///   Email:        nuboheimer@yandex.ru
///   Help:         https://t.me/nuboheimersb/5
///----------------------------------------------------------------------------

///   Version:      0.10.1

using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Threading;
using System.Data;

public class CPHInline
{
    private const int DEFAULT_TIME_TO_ADD = 60; // по умолчанию мы добавляем 60 скунд к времени просмотра.
    public void Init()
    {
        DatabaseManager.InitializeDatabase();
    }

    public bool DropDatabase()
    {
        DatabaseManager.DropDatabase();
        return true;
    }

    public bool AddMessageCount()
    {
        try
        {
            string service = NormalizeService();
            var user = CreateUserFormArgs(service);
            
            // Дополнительная проверка - если у пользователя не инициализированы критичные поля, логируем и прерываем операцию
            if (string.IsNullOrEmpty(user.Service) || string.IsNullOrEmpty(user.ServiceUserId))
            {
                CPH.LogError($"[RankSystem] Critical user data missing. Service: {user.Service}, ServiceUserId: {user.ServiceUserId}");
                return false;
            }
            
            var existingUser = DatabaseManager.GetUserData(
                filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
                parameters: new[]
                {
                     new SQLiteParameter("@Service", user.Service),
                     new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
                }
            ).FirstOrDefault();

            if (existingUser is not null)
            {
                user = existingUser;
                user.MessageCount += 1; // Увеличиваем только для отслеживания в UpsertUser
            }
            else
            {
                user.MessageCount = 1;
            }

            if (!CPH.TryGetArg("coinsToAdd", out long coinsToAdd))
                coinsToAdd = 0;
                
            user.Coins += coinsToAdd; // Только добавляем дельту, UpsertUser корректно обработает
            
            DatabaseManager.UpsertUser(user);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] AddMessageCount Error: {ex}");
            return false;
        }
    }

    public bool AddWatchTime()
    {
        try
        {
            if (!args.ContainsKey("users"))
            {
                CPH.LogWarn("Список пользователей пуст или отсутствует.");
                return false;
            }

            var currentViewers = (List<Dictionary<string, object>>)args["users"];

            if (currentViewers.Count == 0)
            {
                CPH.LogWarn("Список пользователей пуст.");
                return false;            
            }

            string service = NormalizeService();

            if (!CPH.TryGetArg("timeToAdd", out int timeToAdd))
                timeToAdd = DEFAULT_TIME_TO_ADD;

            foreach (var viewer in currentViewers)
            {
                string userName = viewer["userName"].ToString().ToLower();
                string userId = viewer["id"].ToString();

                var user = CreateUserFormArgs(service, userName, userId);

                var existingUser = DatabaseManager.GetUserData(
                    filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
                    parameters: new[]
                    {
                     new SQLiteParameter("@Service", user.Service),
                     new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
                    }
                ).FirstOrDefault();

                if (existingUser is not null)
                    user = existingUser;
                if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd))
                    coinsToAdd = 0;
                user.Coins += coinsToAdd;
                user.WatchTime += timeToAdd;
                DatabaseManager.UpsertUser(user);
            }

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"Ошибка в AddWatchTime: {ex}");
            return false;
        }
    }

    public bool AddFollowDate()
    {
        try
        {
            string service = NormalizeService();
            var user = CreateUserFormArgs(service);
            var existingUser = DatabaseManager.GetUserData(
                filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
                parameters: new[]
                {
                     new SQLiteParameter("@Service", user.Service),
                     new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
                }
            ).FirstOrDefault();

            if (existingUser is not null)
                user = existingUser;
            if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd))
                coinsToAdd = 0;
            if (CPH.TryGetArg("game", out string game)) ; // записываем категорию стрима, если она есть аргументах.

            user.FollowDate = DateTime.Now;
            user.GameWhenFollow = game;
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

    public bool AddCoins()
    {
        try
        {
            string service = NormalizeService();
            var user = CreateUserFormArgs(service);
            var existingUser = DatabaseManager.GetUserData(
                filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
                parameters: new[]
                {
                     new SQLiteParameter("@Service", user.Service),
                     new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
                }
            ).FirstOrDefault();

            if (existingUser is not null)
                user = existingUser;
            if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd))
                coinsToAdd = 0;
            user.Coins += coinsToAdd;
            DatabaseManager.UpsertUser(user);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] AddCoins Error: {ex}");
            return false;
        }
    }

    public bool GetWatchTime()
    {
        try
        {
            string service = NormalizeService();
            var user = CreateUserFormArgs(service);
            var userData = DatabaseManager.GetUserData(
                filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
                parameters: new[]
                {
                     new SQLiteParameter("@Service", user.Service),
                     new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
                }
            ).FirstOrDefault();
            var watchTime = userData?.WatchTime ?? 0;
            string formatedWatchTime = FormatDateTime(watchTime);

            if (watchTime == 0)
            {
                CPH.SetArgument("watchTime", "Время пользователя не найдено!");
            }
            else
            {
                CPH.SetArgument("watchTime", formatedWatchTime);
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
            string service = NormalizeService();
            var user = CreateUserFormArgs(service);
            var userData = DatabaseManager.GetUserData(
                filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
                parameters: new[]
                {
                     new SQLiteParameter("@Service", user.Service),
                     new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
                }
            ).FirstOrDefault();
            var followDate = userData?.FollowDate.ToString("o") ?? string.Empty;
            CPH.SetArgument("followDate", followDate);

            if (followDate.ToString().Equals("0001-01-01T00:00:00.0000000"))
            {
                CPH.SetArgument("followDate", "Нет информации о дате фоллова!");
            }
            else
            {
                CPH.SetArgument("followDate", followDate);
            }
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetFollowDate Error: {ex}");
            return false;
        }
    }

    public bool GetMessageCount()
    {
        try
        {
            string service = NormalizeService();
            var user = CreateUserFormArgs(service);
            var userData = DatabaseManager.GetUserData(
                filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
                parameters: new[]
                {
                     new SQLiteParameter("@Service", user.Service),
                     new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
                }
            ).FirstOrDefault();
            var messageCount = userData?.MessageCount ?? 0;

            if (messageCount == 0)
            {
                CPH.SetArgument("messageCount", "Кажется, пользователь ещё не писал в чат.");
            }
            else
            {
                CPH.SetArgument("messageCount", messageCount);
            }

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetMessageCount Error: {ex}");
            return false;
        }
    }

    public bool GetCoins()
    {
        try
        {
            string service = NormalizeService();
            var user = CreateUserFormArgs(service);
            var userData = DatabaseManager.GetUserData(
                filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
                parameters: new[]
                {
                     new SQLiteParameter("@Service", user.Service),
                     new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
                }
            ).FirstOrDefault();
            var coins = userData?.Coins ?? 0;

            CPH.SetArgument("coins", coins);

            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetCoins Error: {ex}");
            return false;
        }
    }

    public bool GetGameWhenFollow()
    {
        try
        {
            string service = NormalizeService();
            var user = CreateUserFormArgs(service);
            var userData = DatabaseManager.GetUserData(
                filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
                parameters: new[]
                {
                     new SQLiteParameter("@Service", user.Service),
                     new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
                }
            ).FirstOrDefault();
            var gameWhenFollow = userData?.GameWhenFollow ?? string.Empty;

            if (string.IsNullOrEmpty(gameWhenFollow))
            {
                CPH.SetArgument("gameWhenFollow", "Информация об игре не найдена!");
            }
            else
            {
                CPH.SetArgument("gameWhenFollow", gameWhenFollow);
            }
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetGameWhenFollow Error: {ex}");
            return false;
        }
    }

    public bool GetTopViewers()
    {
        try
        {

            if (!CPH.TryGetArg("topType", out string topType)) // если не был настроен тип топа
            {
                topType = "watchtime"; // задаётся тип по времени просмотра.
            }

            if (!CPH.TryGetArg("topCount", out int topCount)) // если не было задано количество позиций в топе
            {
                topCount = 3; // по умолчанию задаётся три.
            }

            var (fieldName, displayName) = topType switch
            {
                "watchtime" => ("WatchTime", "времени просмотра"),
                "messagecount" => ("MessageCount", "количеству сообщений"),
                "coins" => ("Coins", "монетам"),
                _ => throw new ArgumentException("Неизвестный тип топа")
            };

            // Получаем топ пользователей
            var topUsers = DatabaseManager.GetTopUsers(fieldName, topCount);

            if (topUsers.Count == 0)
            {
                CPH.SetArgument("reply", "Топ пуст. Никто ещё не набрал статистики.");
                return true;
            }

            // Формируем результат
            var topEntries = topUsers
                .Select((u, i) => $"{i + 1}. {u.UserName} ({u.Service}) - {FormatValue(u, fieldName)}")
                .ToList();

            CPH.SetArgument("reply", $"Топ по {displayName}: " + string.Join(", ", topEntries));
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetTopViewers Error: {ex}");
            return false;
        }
    }

    private string FormatValue(UserData user, string field)
    {
        return field switch
        {
            "WatchTime" => FormatDateTime(user.WatchTime),
            "MessageCount" => $"{user.MessageCount} сообщ.",
            "Coins" => $"{user.Coins} монет",
            _ => "0"
        };
    }

    public bool ClearUsersCoins()
    {
        List<UserData> users = DatabaseManager.GetUserData();
        foreach (UserData user in users)
        {
            user.Coins = 0 - user.Coins;
            DatabaseManager.UpsertUser(user);
        }

        return true;
    }

    private UserData CreateUserFormArgs(string service, string userName = null, string serviceUserId = null)
    {
        // Если ServiceUserId не передан, пытаемся получить из аргументов
        if (string.IsNullOrEmpty(serviceUserId))
        {
            if (!CPH.TryGetArg("userId", out serviceUserId))
            {
                CPH.TryGetArg("minichat.Data.UserID", out serviceUserId);
            }
        }

        // Если serviceUserId все еще null или пустая строка, создаем временный ID
        if (string.IsNullOrEmpty(serviceUserId))
        {
            CPH.LogWarn($"[RankSystem] ServiceUserId is NULL or empty for service {service}. Using temporary ID.");
            // Создаем временный ID на основе имени пользователя или текущего времени
            serviceUserId = $"temp_{(string.IsNullOrEmpty(userName) ? DateTime.Now.Ticks.ToString() : userName)}";
        }

        // Если UserName не передан, берем из аргументов
        if (string.IsNullOrEmpty(userName) && args.ContainsKey("userName"))
        {
            userName = args["userName"].ToString().ToLower();
        }

        // Если userName все еще null, используем временное имя
        if (string.IsNullOrEmpty(userName))
        {
            userName = $"user_{serviceUserId}";
        }

        return new UserData
        {
            Service = service,
            ServiceUserId = serviceUserId,
            UserName = userName
        };
    }

    private string NormalizeService()
    {
        // TODO: Refactor. Выглядит как говно.
        if (!CPH.TryGetArg("eventSource", out string service))
            if (!CPH.TryGetArg("commandSource", out service)) ;
        if (service.Equals("misc"))
        {
            if (args.ContainsKey("timerId") && (args["timerId"].ToString().Equals("1da45ce2-2383-4431-8b42-b4f3314d2d79") || args["timerName"].ToString().ToLower().Equals("vkvideolive")))
            {
                return "vkvideolive";
            }
        }

        if (service.Equals("command"))
            service = args["commandSource"].ToString();

        return service.Equals("vkplay", StringComparison.OrdinalIgnoreCase) ? "vkvideolive" : service.ToLower();
    }

    public bool SendReply()
    {
        string service = NormalizeService();

        if (!CPH.TryGetArg("reply", out string reply))
        {
            reply = "Стример забыл настроить ответ на команду!";
        }

        if (service.Equals("twitch"))
            CPH.SendMessage(reply);
        else if (service.Equals("youtube"))
            CPH.SendYouTubeMessage(reply);
        else if (service.Equals("trovo"))
            CPH.SendTrovoMessage(reply);
        else
        {
            CPH.SetArgument("message", reply);
            CPH.ExecuteMethod("MiniChat Method Collection", "SendMessageReply");
        }

        return true;
    }

    public string FormatDateTime(long totalSeconds)
    {
        // Преобразуем общее количество секунд в годы, месяцы, дни, часы, минуты и секунды.
        int years = 0;
        int months = 0;
        int days = (int)(totalSeconds / (60 * 60 * 24));
        int hours = (int)((totalSeconds % (60 * 60 * 24)) / (60 * 60));
        int minutes = (int)((totalSeconds % (60 * 60)) / 60);
        int seconds = (int)(totalSeconds % 60);
        if (days >= 365)
        {
            years = days / 365; // Примерно считаем годы
            days %= 365; // Остаток дней после вычисления лет
        }

        if (days >= 30)
        {
            months = days / 30;
            days %= 30; // Остаток дней после вычисления месяцев
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
        return result.Trim(); // Убираем лишние пробелы в конце
    }

    static string GetYearWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "год";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "года";
        return "лет";
    }

    static string GetMonthWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "месяц";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "месяца";
        return "месяцев";
    }

    static string GetDayWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "день";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "дня";
        return "дней";
    }

    static string GetHourWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "час";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "часа";
        return "часов";
    }

    static string GetMinuteWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "минуту";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "минуты";
        return "минут";
    }

    static string GetSecondWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "секунду";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "секунды";
        return "секунд";
    }
}

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

public static class DatabaseManager
{
    // TODO: Надо что-то придумать с хардкодом пути до базы. Но, на первый взгляд, отсюда не получить аргументы среды выполнения.
    private static readonly string DbPath = "RankSystem.db";
    private static readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
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
                    cmd.CommandText = @"
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                    PRAGMA busy_timeout = 5000;
                    PRAGMA cache_size = -2000;
                    PRAGMA temp_store = MEMORY;
                    PRAGMA wal_autocheckpoint = 1000;
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
                    );
                    CREATE TABLE IF NOT EXISTS UserNameHistory (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        UUID TEXT NOT NULL,
                        OldUserName TEXT,
                        NewUserName TEXT,
                        ChangeDate TEXT NOT NULL,
                        FOREIGN KEY (UUID) REFERENCES Users(UUID)
                    );";
                    cmd.ExecuteNonQuery();
                }
            }
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
            // Инициализация теперь выполняется вне блокировки
        }
        finally
        {
            _lock.ExitWriteLock();
        }
        // Реинициализация БД после освобождения блокировки
        InitializeDatabase();
    }

    public static void UpsertUser(UserData user)
    {
        // Определим, является ли это добавлением сообщения
        bool isMessageIncrement = false;
        long coinsToAdd = 0;
        
        // Получаем данные о пользователе ДО входа в блокировку записи
        var existingUser = GetUserData(
            filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
            parameters: new[]
            {
                new SQLiteParameter("@Service", user.Service),
                new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
            }
        ).FirstOrDefault();

        // Расчет дельты для инкрементов
        if (existingUser != null)
        {
            if (user.MessageCount > existingUser.MessageCount)
            {
                isMessageIncrement = true;
            }
            
            coinsToAdd = user.Coins - existingUser.Coins;
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
                                
                                if (user.FollowDate > DateTime.MinValue)
                                {
                                    updateQuery += ", FollowDate = @FollowDate, GameWhenFollow = @GameWhenFollow";
                                }
                                
                                updateQuery += " WHERE Service = @Service AND ServiceUserId = @ServiceUserId";
                                
                                cmd.CommandText = updateQuery;
                                cmd.Parameters.AddWithValue("@Service", user.Service);
                                cmd.Parameters.AddWithValue("@ServiceUserId", user.ServiceUserId);
                                cmd.Parameters.AddWithValue("@UserName", user.UserName);
                                cmd.Parameters.AddWithValue("@WatchTime", user.WatchTime);
                                cmd.Parameters.AddWithValue("@CoinsToAdd", coinsToAdd);
                                
                                if (user.FollowDate > DateTime.MinValue)
                                {
                                    cmd.Parameters.AddWithValue("@FollowDate", user.FollowDate.ToString("o"));
                                    cmd.Parameters.AddWithValue("@GameWhenFollow", user.GameWhenFollow ?? (object)DBNull.Value);
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
                            users.Add(new UserData
                            {
                                UUID = reader["UUID"].ToString(),
                                Service = reader["Service"].ToString(),
                                ServiceUserId = reader["ServiceUserId"].ToString(),
                                UserName = reader["UserName"].ToString(),
                                WatchTime = Convert.ToInt64(reader["WatchTime"]),
                                FollowDate = DateTime.Parse(reader["FollowDate"].ToString()),
                                MessageCount = Convert.ToInt64(reader["MessageCount"]),
                                Coins = Convert.ToInt64(reader["Coins"]),
                                GameWhenFollow = reader["GameWhenFollow"] as string
                            });
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

        return GetUserData(
            filter: $"{topType} > 0 ORDER BY {orderBy} LIMIT @limit",
            parameters: new[] { new SQLiteParameter("@limit", limit) }
        );
    }
}