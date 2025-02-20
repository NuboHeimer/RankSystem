///----------------------------------------------------------------------------
///   Module:       RankSystemDB
///   Author:       NuboHeimer (https://live.vkvideo.ru/nuboheimer)
///   Email:        nuboheimer@yandex.ru
///----------------------------------------------------------------------------

///   Version:      0.1.1

using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Threading;

public class CPHInline
{
    public void Init()
    {
        DatabaseManager.InitializeDatabase();
    }

    public bool AddMessageCount()
    {
        try
        {
            var service = NormalizeService(args["eventSource"].ToString());
            var user = GetUserFromArgs(service);

            user.MessageCount = 1;

            if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd))
                coinsToAdd = 0;
            user.Coins = coinsToAdd;

            DatabaseManager.UpsertUser(user);
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"AddMessageCount Error: {ex}");
            return false;
        }
    }

    private UserData GetUserFromArgs(string service)
    {
        if (!CPH.TryGetArg("userId", out string UserId))
            CPH.TryGetArg("minichat.Data.UserID", out UserId);
        return new UserData
        {
            Service = service,
            UserId = UserId,
            UserName = args["userName"].ToString().ToLower()
        };
    }

    private string NormalizeService(string service)
    {
        return service.Equals("vkplay", StringComparison.OrdinalIgnoreCase) ? "vkvideolive" : service.ToLower();
    }
}

public class UserData
{
    public string Service { get; set; }
    public string UserId { get; set; }
    public string UserName { get; set; }
    public TimeSpan WatchTime { get; set; }
    public DateTime FollowDate { get; set; }
    public int MessageCount { get; set; }
    public int Coins { get; set; }
    public string GameWhenFollow { get; set; }
}

public static class DatabaseManager
{
    private static SQLiteConnection _connection;
    private static readonly string DbPath = "RankSystem.db"; //Задаём путь до базы данных.
    private static readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

    public static void InitializeDatabase()
    {
        _lock.EnterWriteLock();
        try
        {
            _connection = new SQLiteConnection($"Data Source={DbPath};Version=3;");
            _connection.Open();
            
            using (var cmd = new SQLiteCommand(_connection))
            {
                cmd.CommandText = @"
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                    
                    CREATE TABLE IF NOT EXISTS Users (
                        Service TEXT NOT NULL,
                        UserId TEXT NOT NULL,
                        UserName TEXT NOT NULL,
                        WatchTime TEXT NOT NULL,
                        FollowDate TEXT NOT NULL,
                        MessageCount INTEGER DEFAULT 0,
                        Coins INTEGER DEFAULT 0,
                        GameWhenFollow TEXT,
                        PRIMARY KEY (Service, UserId)
                    );";
                cmd.ExecuteNonQuery();
            }
        }
        catch (Exception ex)
        {
            // TODO логгер. Можно подсмотреть у Пликода.
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public static void UpsertUser(UserData user)
    {
        _lock.EnterWriteLock();
        try
        {
            using (var transaction = _connection.BeginTransaction())
            using (var cmd = new SQLiteCommand(_connection))
            {
                cmd.CommandText = @"
                    INSERT OR REPLACE INTO Users 
                    (Service, UserId, UserName, WatchTime, FollowDate, MessageCount, Coins, GameWhenFollow)
                    VALUES (
                        @Service, @UserId, @UserName, 
                        @WatchTime, @FollowDate, 
                        COALESCE((SELECT MessageCount FROM Users WHERE Service = @Service AND UserId = @UserId), 0) + @MessageCountInc,
                        COALESCE((SELECT Coins FROM Users WHERE Service = @Service AND UserId = @UserId), 0) + @CoinsInc,
                        COALESCE(@GameWhenFollow, (SELECT GameWhenFollow FROM Users WHERE Service = @Service AND UserId = @UserId))
                    )";

                cmd.Parameters.AddWithValue("@Service", user.Service);
                cmd.Parameters.AddWithValue("@UserId", user.UserId);
                cmd.Parameters.AddWithValue("@UserName", user.UserName);
                cmd.Parameters.AddWithValue("@WatchTime", user.WatchTime.ToString());
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