using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Threading;

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
    private static readonly string DbPath = "users.db";
    private static readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

    static DatabaseManager()
    {
        InitializeDatabase();
    }

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
            throw;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public static List<UserData> SearchUsers(string service, string searchPattern)
    {
        _lock.EnterReadLock();
        try
        {
            using (var cmd = new SQLiteCommand(_connection))
            {
                cmd.CommandText = @"
                    SELECT * FROM Users 
                    WHERE 
                        Service = @Service AND 
                        (UserId LIKE @Pattern OR UserName LIKE @Pattern)";
                cmd.Parameters.AddWithValue("@Service", service);
                cmd.Parameters.AddWithValue("@Pattern", $"%{searchPattern}%");
                using (var reader = cmd.ExecuteReader())
                {
                    return ReadUsers(reader).ToList();
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private static IEnumerable<UserData> ReadUsers(SQLiteDataReader reader)
    {
        while (reader.Read())
        {
            yield return new UserData
            {
                Service = reader["Service"].ToString(),
                UserId = reader["UserId"].ToString(),
                UserName = reader["UserName"].ToString(),
                WatchTime = TimeSpan.Parse(reader["WatchTime"].ToString()),
                FollowDate = DateTime.Parse(reader["FollowDate"].ToString()),
                MessageCount = Convert.ToInt32(reader["MessageCount"]),
                Coins = Convert.ToInt32(reader["Coins"]),
                GameWhenFollow = reader["GameWhenFollow"]?.ToString()
            };
        }
    }

    public static void ResetCounter(string counterName, string service = null)
    {
        _lock.EnterWriteLock();
        try
        {
            using (var cmd = new SQLiteCommand(_connection))
            {
                cmd.CommandText = service != null ? $"UPDATE Users SET {counterName} = 0 WHERE Service = @Service" : $"UPDATE Users SET {counterName} = 0";
                if (service != null)
                    cmd.Parameters.AddWithValue("@Service", service);
                cmd.ExecuteNonQuery();
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
}

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