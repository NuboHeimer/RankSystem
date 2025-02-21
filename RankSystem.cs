///----------------------------------------------------------------------------
///   Module:       RankSystemDB
///   Author:       NuboHeimer (https://live.vkvideo.ru/nuboheimer)
///   Email:        nuboheimer@yandex.ru
///----------------------------------------------------------------------------
 
///   Version:      0.4.0
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Threading;

public class CPHInline
{

    private const int DEFAULT_TIME_TO_ADD = 60; // по умолчанию мы добавляем 60 секунд к времени просмотра.
    public void Init() {
        DatabaseManager.InitializeDatabase();
    }

    public bool AddMessageCount() {
        try {
            
            string service = NormalizeService();
            var user = GetUserFromArgs(service);

            if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd))
                coinsToAdd = 0;

            user.MessageCount = 1;
            user.Coins = coinsToAdd;

            DatabaseManager.UpsertUser(user);

            return true;
        } catch (Exception ex) {
            CPH.LogError($"[RankSystem] AddMessageCount Error: {ex}");
            return false;
        }
    }

    public bool AddWatchTime() {
        try {
            if (!args.ContainsKey("users"))
                CPH.LogWarn("Список пользователей пуст или отсутствует.");

            var currentViewers = (List<Dictionary<string, object>>)args["users"];

            if (currentViewers.Count == 0)
                CPH.LogWarn("Список пользователей пуст.");

            string service = NormalizeService();

            if (!CPH.TryGetArg("timeToAdd", out int timeToAdd))
                timeToAdd = DEFAULT_TIME_TO_ADD;

            foreach (var viewer in currentViewers) {
                
                string userName = viewer["userName"].ToString().ToLower();
                string userId = viewer["id"].ToString();
          
                var user = GetUserFromArgs(service, userName, userId);

                if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd))
                    coinsToAdd = 0;

                user.Coins = coinsToAdd;
                user.WatchTime = timeToAdd;

                DatabaseManager.UpsertUser(user);
            }

        return true;
        } catch (Exception ex) {
            CPH.LogError($"Ошибка в AddWatchTime: {ex}");
            return false;
        }
    }

    public bool AddFollowDate() {
        try {
            
            string service = NormalizeService();
            var user = GetUserFromArgs(service);

            if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd))
                coinsToAdd = 0;
            
            if (CPH.TryGetArg("game", out string game)) // записываем категорию стрима, если она есть аргументах.

            user.FollowDate = DateTime.Now;
            user.GameWhenFollow = game;
            user.Coins = coinsToAdd;

            DatabaseManager.UpsertUser(user);

            return true;
        } catch (Exception ex) {
            CPH.LogError($"[RankSystem] AddFollowDate Error: {ex}");
            return false;
        }
    }

    public bool AddCoins() {
        try {
            
            string service = NormalizeService();
            var user = GetUserFromArgs(service);

            if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd))
                coinsToAdd = 0;

            user.Coins = coinsToAdd;

            DatabaseManager.UpsertUser(user);

            return true;
        } catch (Exception ex) {
            CPH.LogError($"[RankSystem] AddCoins Error: {ex}");
            return false;
        }
    }

    private UserData GetUserFromArgs(string service, string userName, string ServiceUserId) {
        
        return new UserData {
            
            Service = service,
            ServiceUserId = ServiceUserId,
            UserName = userName
        };
    }

    private UserData GetUserFromArgs(string service) {

        if (!CPH.TryGetArg("userId", out string ServiceUserId))
            CPH.TryGetArg("minichat.Data.UserID", out ServiceUserId);

        return new UserData {

            Service = service,
            ServiceUserId = ServiceUserId,
            UserName = args["userName"].ToString().ToLower()

        };
    }

    private string NormalizeService() {
        // TODO: Refactor. Выглядит как говно.
        if (!CPH.TryGetArg("eventSource", out string service))
                if (!CPH.TryGetArg("commandSource", out service))

        if (service.Equals("misc")) {
            if (args.ContainsKey("timerId") && 
                (args["timerId"].ToString().Equals("1da45ce2-2383-4431-8b42-b4f3314d2d79") || 
                 args["timerName"].ToString().ToLower().Equals("vkvideolive"))) { service = "vkvideolive";
                 }
        }
        return service.Equals("vkplay", StringComparison.OrdinalIgnoreCase) ? "vkvideolive" : service.ToLower();
    }
}

public class UserData
{
    public string Service { get; set; }
    public string ServiceUserId { get; set; }
    public string UserName { get; set; }
    public long WatchTime { get; set; }
    public DateTime FollowDate { get; set; }
    public long MessageCount { get; set; }
    public long Coins { get; set; }
    public string GameWhenFollow { get; set; }
}

public static class DatabaseManager
{
    private static SQLiteConnection _connection;

    // TODO: Надо что-то придумать с хардкодом пути до базы. Но, на первый взгляд, отсюда не получить аргументы среды выполнения.
    private static readonly string DbPath = "RankSystem.db";
    private static readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
    public static void InitializeDatabase() {

        _lock.EnterWriteLock();

        try {

            _connection = new SQLiteConnection($"Data Source={DbPath};Version=3;");
            _connection.Open();
            using (var cmd = new SQLiteCommand(_connection)) {
                cmd.CommandText = @"
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                    
                    CREATE TABLE IF NOT EXISTS Users (
                        Service TEXT NOT NULL,
                        ServiceUserId TEXT NOT NULL,
                        UserName TEXT NOT NULL,
                        WatchTime LONG DEFAULT 0,
                        FollowDate TEXT NOT NULL,
                        MessageCount LONG DEFAULT 0,
                        Coins LONG DEFAULT 0,
                        GameWhenFollow TEXT,
                        PRIMARY KEY (Service, ServiceUserId)
                    );";
                cmd.ExecuteNonQuery();
            }
        } catch (Exception ex) {
            // TODO: Добавить логгер. Можно подсмотреть у Пликода.
        } finally {
            _lock.ExitWriteLock();
        }
    }

    public static void UpsertUser(UserData user) {

        _lock.EnterWriteLock();

        try {
            using (var transaction = _connection.BeginTransaction())
            using (var cmd = new SQLiteCommand(_connection)) {
                
                cmd.CommandText = @"
                    INSERT OR REPLACE INTO Users 
                    (Service, ServiceUserId, UserName, WatchTime, FollowDate, MessageCount, Coins, GameWhenFollow)
                    VALUES (
                        @Service, @ServiceUserId, @UserName, 
                        COALESCE((SELECT WatchTime FROM Users WHERE Service = @Service AND ServiceUserId = @ServiceUserId), 0) + @WatchTimeInc,
                        @FollowDate, 
                        COALESCE((SELECT MessageCount FROM Users WHERE Service = @Service AND ServiceUserId = @ServiceUserId), 0) + @MessageCountInc,
                        COALESCE((SELECT Coins FROM Users WHERE Service = @Service AND ServiceUserId = @ServiceUserId), 0) + @CoinsInc,
                        COALESCE(@GameWhenFollow, (SELECT GameWhenFollow FROM Users WHERE Service = @Service AND ServiceUserId = @ServiceUserId))
                    )";
                cmd.Parameters.AddWithValue("@Service", user.Service);
                cmd.Parameters.AddWithValue("@ServiceUserId", user.ServiceUserId);
                cmd.Parameters.AddWithValue("@UserName", user.UserName);
                cmd.Parameters.AddWithValue("@WatchTimeInc", user.WatchTime);
                cmd.Parameters.AddWithValue("@FollowDate", user.FollowDate.ToString("o"));
                cmd.Parameters.AddWithValue("@MessageCountInc", user.MessageCount);
                cmd.Parameters.AddWithValue("@CoinsInc", user.Coins);
                cmd.Parameters.AddWithValue("@GameWhenFollow", user.GameWhenFollow);
                cmd.ExecuteNonQuery();
                transaction.Commit();
            }
        }
        catch (SQLiteException ex)
        {
            throw; // TODO логгер. Можно подсмотреть у Пликода.
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
}