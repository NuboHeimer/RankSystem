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
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.IO;
using Newtonsoft.Json;
using System.Windows.Forms;
using System.Drawing;
using System.ComponentModel;

public class CPHInline
{
    private const int DEFAULT_TIME_TO_ADD = 60; // по умолчанию мы добавляем 60 скунд к времени просмотра.
    private const long DEFAULT_COINS_TO_ADD = 0; // по умолчанию мы добавляем 0 монет.
    private const int DEFAULT_TOP_COUNT = 3; // по умолчанию задаётся 3 позиции в топе.

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
            string service = RankSystemInternal.NormalizeService(this);
            var user = CreateUserFormArgs(service);
            if (string.IsNullOrEmpty(user.Service) || string.IsNullOrEmpty(user.ServiceUserId))
            {
                CPH.LogError($"[RankSystem][AddMessageCount] Critical user data missing. Service: {user.Service}, ServiceUserId: {user.ServiceUserId}");
                return false;
            }

            var existingUser = DatabaseManager.GetUserData(filter: "Service = @Service AND ServiceUserId = @ServiceUserId", parameters: new[] { new SQLiteParameter("@Service", user.Service), new SQLiteParameter("@ServiceUserId", user.ServiceUserId) }).FirstOrDefault();
            if (existingUser is not null)
            {
                user = existingUser;
                user.MessageCount += 1;
            }
            else
            {
                user.MessageCount = 1;
            }

            if (!CPH.TryGetArg("coinsToAdd", out long coinsToAdd))
                coinsToAdd = DEFAULT_COINS_TO_ADD;
            user.Coins += coinsToAdd;
            DatabaseManager.UpsertUser(user);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem][AddMessageCount] Error: {ex}");
            return false;
        }
    }

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
                timeToAdd = DEFAULT_TIME_TO_ADD;
            foreach (var viewer in currentViewers)
            {
                string userName = viewer["userName"].ToString().ToLower();
                string userId = viewer["id"].ToString();
                var user = CreateUserFormArgs(service, userName, userId);
                var existingUser = DatabaseManager.GetUserData(filter: "Service = @Service AND ServiceUserId = @ServiceUserId", parameters: new[] { new SQLiteParameter("@Service", user.Service), new SQLiteParameter("@ServiceUserId", user.ServiceUserId) }).FirstOrDefault();
                if (existingUser is not null)
                    user = existingUser;
                if (!CPH.TryGetArg("coinsToAdd", out long coinsToAdd))
                    coinsToAdd = DEFAULT_COINS_TO_ADD;
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
            string service = RankSystemInternal.NormalizeService(this);
            var user = CreateUserFormArgs(service);
            var existingUser = DatabaseManager.GetUserData(filter: "Service = @Service AND ServiceUserId = @ServiceUserId", parameters: new[] { new SQLiteParameter("@Service", user.Service), new SQLiteParameter("@ServiceUserId", user.ServiceUserId) }).FirstOrDefault();
            if (existingUser is not null)
                user = existingUser;
            if (!CPH.TryGetArg("coinsToAdd", out long coinsToAdd))
                coinsToAdd = DEFAULT_COINS_TO_ADD;
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

    public bool GetMessageCount()
    {
        try
        {
            long messageCount = RankSystemInternal.GetMessageCount(this);
            CPH.SetArgument("messageCount", messageCount);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetMessageCount Error: {ex}");
            return false;
        }
    }

    public bool GetWatchTime()
    {
        try
        {
            long watchTime = RankSystemInternal.GetWatchTime(this);
            CPH.SetArgument("watchTime", watchTime == 0 ? 0 : RankSystemInternal.FormatDateTime(watchTime));
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
            DateTime followDate = RankSystemInternal.GetFollowDate(this);
            CPH.SetArgument("followDate", followDate == DateTime.MinValue ? "неизвестно когда" : followDate.ToString("o"));
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
            string gameWhenFollow = RankSystemInternal.GetGameWhenFollow(this);
            CPH.SetArgument("gameWhenFollow", string.IsNullOrEmpty(gameWhenFollow) ? "игры нет" : gameWhenFollow);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] GetGameWhenFollow Error: {ex}");
            return false;
        }
    }

    public bool AddCoins(long? coinsToAdd = null)
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = CreateUserFormArgs(service);
            var existingUser = DatabaseManager.GetUserData(filter: "Service = @Service AND ServiceUserId = @ServiceUserId", parameters: new[] { new SQLiteParameter("@Service", user.Service), new SQLiteParameter("@ServiceUserId", user.ServiceUserId) }).FirstOrDefault();
            if (existingUser is not null)
                user = existingUser;
            long coinsFromArgs = DEFAULT_COINS_TO_ADD;
            if (!coinsToAdd.HasValue && !CPH.TryGetArg("coinsToAdd", out coinsFromArgs))
                coinsFromArgs = DEFAULT_COINS_TO_ADD;
            user.Coins += coinsToAdd ?? coinsFromArgs;
            DatabaseManager.UpsertUser(user);
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
            long coins = RankSystemInternal.GetCoins(this);
            CPH.SetArgument("coins", coins);
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
                    AddCoins(-actionCurrency);
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
        List<UserData> users = DatabaseManager.GetUserData();
        foreach (UserData user in users)
        {
            user.Coins = 0 - user.Coins;
            DatabaseManager.UpsertUser(user);
        }

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
                topCount = 3;
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

    public UserData CreateUserFormArgs(string service, string userName = null, string serviceUserId = null)
    {
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

    public bool SendReply()
    {
        string service = RankSystemInternal.NormalizeService(this);
        if (!CPH.TryGetArg("reply", out string reply))
        {
            reply = "Стример забыл настроить ответ на команду!";
        }

        if (service.Equals("twitch"))
            CPH.SendMessage(reply);
        else if (service.Equals("Kick"))
        {
            CPH.SetArgument("message", reply);
            CPH.ExecuteMethod("Kick", "SendMessage");
        }
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
                var existingUser = DatabaseManager.GetUserData(
                    filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
                    parameters: new[] {
                        new SQLiteParameter("@Service", data.Service == "Unknown" ? "vkvideolive" : data.Service.ToLower()),
                        new SQLiteParameter("@ServiceUserId", data.UserID)
                    }
                ).FirstOrDefault();

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
}

// Класс для десериализации данных из Live.json
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

public class AvatarData
{
    public string Default { get; set; }
    public string Large { get; set; }
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

public class UserNameHistory
{
    public long Id { get; set; }
    public string UUID { get; set; }
    public string OldUserName { get; set; }
    public string NewUserName { get; set; }
    public DateTime ChangeDate { get; set; }
}

public static class DatabaseManager
{
    // TODO: Надо что-то придумать с хардкодом пути до базы. Но, на первый взгляд, отсюда не получить аргументы среды выполнения.
    private static readonly string DbPath = "RankSystem.db";
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
                    cmd.CommandText = @"
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                    PRAGMA busy_timeout = 5000;
                    PRAGMA cache_size = -2000;
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
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        InitializeDatabase();
    }

    public static void UpsertUser(UserData user)
    {
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

        // Если изменился никнейм, добавляем запись в историю после завершения основной транзакции
        if (oldUserName != null && uuid != null)
        {
            AddUserNameHistory(uuid, oldUserName, user.UserName);
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
                    SELECT * FROM UserNameHistory 
                    WHERE UUID = @UUID 
                    ORDER BY ChangeDate DESC";

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
                                ChangeDate = DateTime.Parse(reader["ChangeDate"].ToString())
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
}

public class RankSystemInternal
{
    public static long GetCoins(CPHInline cph)
    {
        string service = NormalizeService(cph);
        var user = cph.CreateUserFormArgs(service);
        var userData = DatabaseManager.GetUserData(
            filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
            parameters: new[] {
                new SQLiteParameter("@Service", user.Service),
                new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
            }
        ).FirstOrDefault();

        return userData?.Coins ?? 0;
    }

    public static long GetWatchTime(CPHInline cph)
    {
        string service = NormalizeService(cph);
        var user = cph.CreateUserFormArgs(service);
        var userData = DatabaseManager.GetUserData(
            filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
            parameters: new[] {
                new SQLiteParameter("@Service", user.Service),
                new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
            }
        ).FirstOrDefault();

        return userData?.WatchTime ?? 0;
    }

    public static DateTime GetFollowDate(CPHInline cph)
    {
        string service = NormalizeService(cph);
        var user = cph.CreateUserFormArgs(service);
        var userData = DatabaseManager.GetUserData(
            filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
            parameters: new[] {
                new SQLiteParameter("@Service", user.Service),
                new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
            }
        ).FirstOrDefault();

        return userData?.FollowDate ?? DateTime.MinValue;
    }

    public static long GetMessageCount(CPHInline cph)
    {
        string service = NormalizeService(cph);
        var user = cph.CreateUserFormArgs(service);
        var userData = DatabaseManager.GetUserData(
            filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
            parameters: new[] {
                new SQLiteParameter("@Service", user.Service),
                new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
            }
        ).FirstOrDefault();

        return userData?.MessageCount ?? 0;
    }

    public static string GetGameWhenFollow(CPHInline cph)
    {
        string service = NormalizeService(cph);
        var user = cph.CreateUserFormArgs(service);
        var userData = DatabaseManager.GetUserData(
            filter: "Service = @Service AND ServiceUserId = @ServiceUserId",
            parameters: new[] {
                new SQLiteParameter("@Service", user.Service),
                new SQLiteParameter("@ServiceUserId", user.ServiceUserId)
            }
        ).FirstOrDefault();

        return userData?.GameWhenFollow ?? string.Empty;
    }

    public static string NormalizeService(CPHInline cph)
    {
        // TODO: Refactor. Выглядит как говно.
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

    public static string FormatDateTime(long totalSeconds)
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
        allUsers = DatabaseManager.GetUserData();
        bindingUsers = new BindingList<UserData>(allUsers);
        usersGrid.DataSource = bindingUsers;
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
        txtGameWhenFollow.Text = user.GameWhenFollow;
    }

    private UserData GetUserFromFields()
    {
        return new UserData
        {
            UUID = txtUUID.Text,
            Service = txtService.Text,
            ServiceUserId = txtServiceUserId.Text,
            UserName = txtUserName.Text,
            WatchTime = long.TryParse(txtWatchTime.Text, out var wt) ? wt : 0,
            FollowDate = DateTime.TryParse(txtFollowDate.Text, out var fd) ? fd : DateTime.MinValue,
            MessageCount = long.TryParse(txtMessageCount.Text, out var mc) ? mc : 0,
            Coins = long.TryParse(txtCoins.Text, out var c) ? c : 0,
            GameWhenFollow = txtGameWhenFollow.Text
        };
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
        DatabaseManager.UpsertUser(user);
        LoadUsers();
        ClearAllFilters();
        MessageBox.Show("User saved.");
    }

    private void BtnRefresh_Click(object sender, EventArgs e)
    {
        LoadUsers();
        ClearAllFilters();
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
        txtGameWhenFollow.Text = "";
    }

    private void ClearAllFilters()
    {
        foreach (DataGridViewColumn column in usersGrid.Columns)
        {
            if (column is DataGridViewFilteredTextBoxColumn filteredColumn)
            {
                filteredColumn.ClearFilter();
            }
        }
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
    }

    public void HideFilter()
    {
        filterPanel.Visible = false;
        isFilterVisible = false;
    }

    public void ClearFilter()
    {
        filterTextBox.Text = "";
        ApplyFilter("");
    }

    public string GetFilterText()
    {
        return filterTextBox.Text;
    }

    private void FilterTextBox_TextChanged(object sender, EventArgs e)
    {
        ApplyFilter(filterTextBox.Text);
    }

    private void FilterTextBox_KeyDown(object sender, KeyEventArgs e)
    {
        if (e.KeyCode == Keys.Escape)
        {
            HideFilter();
        }
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

            // Обновляем DataSource
            DataGridView.DataSource = new BindingList<UserData>(filteredList);
        }
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

        // Рисуем фон кнопки
        graphics.FillRectangle(Brushes.LightBlue, buttonRect);
        graphics.DrawRectangle(Pens.DarkBlue, buttonRect);

        // Рисуем иконку фильтра
        using (var font = new Font("Arial", 10))
        {
            graphics.DrawString("🔍", font, Brushes.Black, buttonRect, new StringFormat { Alignment = StringAlignment.Center, LineAlignment = StringAlignment.Center });
        }

        // Если фильтр активен, рисуем индикатор
        if (isFilterVisible)
        {
            graphics.FillEllipse(Brushes.Red, new Rectangle(cellBounds.Right - 8, cellBounds.Top + 2, 6, 6));
        }
    }
}