///----------------------------------------------------------------------------
///   Module:       RankSystem
///   Author:       NuboHeimer (https://live.vkvideo.ru/nuboheimer)
///   Email:        nuboheimer@yandex.ru
///----------------------------------------------------------------------------
 
///   Version:      0.3.0
using System;
using System.Collections.Generic;
using Newtonsoft.Json;

public class CPHInline
{
    private const int DEFAULT_TIME_TO_ADD = 60; // по умолчанию мы добавляем 60 секунд к времени просмотра.
    private bool InitializeUserGlobalVar(string viewerVariableName, string eventSource)
    {
        var userRankCollection = new List<KeyValuePair<string, string>>();
        userRankCollection.Add(new KeyValuePair<string, string>("WatchTime", "0"));
        userRankCollection.Add(new KeyValuePair<string, string>("MessageCount", "0"));
        userRankCollection.Add(new KeyValuePair<string, string>("FollowDate", null));
        userRankCollection.Add(new KeyValuePair<string, string>("Rank", null));

        CPH.SetGlobalVar(viewerVariableName, JsonConvert.SerializeObject(userRankCollection), true);

        return true;
    }

    public bool AddWatchTime()
    {
        string eventSource = "Test";
        var userRankCollection = new List<KeyValuePair<string, string>>();

        if (args["eventSource"].ToString().ToLower().Equals("misc")) // если в eventSource лежит misc -- значит это не дефолтный PresentVieewr.
        {
            if (args.ContainsKey("timerId")) // если это кастомный таймер -- надо понять, какой именно.
            {
                if (args["timerId"].ToString().Equals("1da45ce2-2383-4431-8b42-b4f3314d2d79") || args["timerName"].ToString().ToLower().Equals("vkvideolive"))
                    eventSource = "vkvideolive";
            }
        }
        else
            eventSource = args["eventSource"].ToString().ToLower(); // иначе просто берём источник события.

        if (args.ContainsKey("users")) // защита от дурака с пустым аргументом списка пользователей.
        {
            var currentViewers = (List<Dictionary<string, object>>)args["users"];

            if (currentViewers.Count == 0)
                return true; // выходим, если список зрителей всё же пустой.

            if (!CPH.TryGetArg("timeToAdd", out int timeToAdd)) // записываем, сколько добавлять времени, если это задано в настройках экшене
                timeToAdd = DEFAULT_TIME_TO_ADD; // или записываем дефолтное значение

            foreach (var viewer in currentViewers) // проходимся по зрителям в списке.
            {
                string userName = viewer["userName"].ToString().ToLower();
                string userId = viewer["id"].ToString();
                
                if (CPH.TryGetArg("viewersBlackList", out string tempViewersBlackList)) // проверяем чёрный список зрителей.
                {
                    List<string> viewersBlackList = new List<string>(tempViewersBlackList.ToLower().Split(';'));
                    if (viewersBlackList.Contains(userName))
                        continue; // пропускаем итерацию если существует чёрный список зрителей и текущий в нём есть.
                }

                string viewerVariableName = userName + userId + eventSource + "RankSystem";

                if (CPH.GetGlobalVar<string>(viewerVariableName, true) == null) // если для текущего зрителя ещё нет глобальной переменной -- инициализируем её.
                    InitializeUserGlobalVar(viewerVariableName, eventSource);

                string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
                userRankCollection = new List<KeyValuePair<string, string>>();
                userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
                string keyToUpdate = "WatchTime";
                int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToUpdate);
                
                if (index == -1)
                    userRankCollection.Add(new KeyValuePair<string, string>(keyToUpdate, timeToAdd.ToString())); // если у пользователя ещё нет времени просмотра задаём начальное значение.
                else
                {
                    string newValue = (int.Parse(userRankCollection[index].Value) + timeToAdd).ToString(); // добавляем время просмотра
                    userRankCollection[index] = new KeyValuePair<string, string>(keyToUpdate, newValue);
                }

                CPH.SetGlobalVar(viewerVariableName, JsonConvert.SerializeObject(userRankCollection), true);

                if (CPH.TryGetArg("coinsToAdd", out int coinsToAdd)) // записываем значение валюты за минуты просмотра, если она задана в настройках экшена
                    AddCoins(coinsToAdd, eventSource, userName, userId);

            }
        }

        return true;
    }

    public bool AddMessageCount()
    {
        var userRankCollection = new List<KeyValuePair<string, string>>();
        string eventSource = args["eventSource"].ToString().ToLower();
        
        if (eventSource.Equals("vkplay"))
            eventSource = "vkvideolive";
        
        string userName = args["userName"].ToString().ToLower();
        if (CPH.TryGetArg("viewersBlackList", out string tempViewersBlackList))
        {
            List<string> viewersBlackList = new List<string>(tempViewersBlackList.ToLower().Split(';'));
            if (viewersBlackList.Contains(userName))
                return false;
        }

        string userId = "";

        if (!CPH.TryGetArg("userId", out userId))
            CPH.TryGetArg("minichat.Data.UserID", out userId);


        string viewerVariableName = userName + userId + eventSource + "RankSystem";
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) == null)
            InitializeUserGlobalVar(viewerVariableName, eventSource);

        string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
        userRankCollection = new List<KeyValuePair<string, string>>();
        userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
        string keyToUpdate = "MessageCount";
        int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToUpdate);
        
        if (index == -1)
            userRankCollection.Add(new KeyValuePair<string, string>(keyToUpdate, "1"));

        if (index != -1)
        {
            string newValue = (int.Parse(userRankCollection[index].Value) + 1).ToString();
            userRankCollection[index] = new KeyValuePair<string, string>(keyToUpdate, newValue);
            
        }

        CPH.SetGlobalVar(viewerVariableName, JsonConvert.SerializeObject(userRankCollection), true);

        if (CPH.TryGetArg("coinsToAdd", out int coinsToAdd)) // записываем значение валюты за сообщение, если она задана в настройках экшена
            AddCoins(coinsToAdd, eventSource, userName, userId);

        

        return true;
    }

    public bool AddFollowDate()
    {
        var userRankCollection = new List<KeyValuePair<string, string>>();
        string eventSource = args["eventSource"].ToString().ToLower();
        if (eventSource.Equals("vkplay"))
            eventSource = "vkvideolive";
        string userName = args["userName"].ToString().ToLower();
        
        string userId = "";
        
        if (!CPH.TryGetArg("userId", out userId))
            CPH.TryGetArg("minichat.Data.UserID", out userId);

        string viewerVariableName = userName + userId + eventSource + "RankSystem";
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) == null)
            InitializeUserGlobalVar(viewerVariableName, eventSource);

        string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
        userRankCollection = new List<KeyValuePair<string, string>>();
        userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
        string keyToUpdate = "FollowDate";
        int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToUpdate);
        
        if (index == -1)
            userRankCollection.Add(new KeyValuePair<string, string>(keyToUpdate, DateTime.Now.ToString()));

        if (index != -1)
        {
            string newValue = DateTime.Now.ToString();
            userRankCollection[index] = new KeyValuePair<string, string>(keyToUpdate, newValue);
        }

        CPH.SetGlobalVar(viewerVariableName, JsonConvert.SerializeObject(userRankCollection), true);

        if (CPH.TryGetArg("game", out string game)){ // записываем категорию стрима, если она есть аргументах.
            AddGameWhenFollow(game, eventSource, userName, userId);
        }

        if (CPH.TryGetArg("coinsToAdd", out int coinsToAdd)) // записываем значение валюты за фоллов, если она задана в настройках экшена
            AddCoins(coinsToAdd, eventSource, userName, userId);

        

        return true;
    }

    private bool AddCoins(int coinsToAdd, string eventSource, string targetUser, string userId)
    {
        var userRankCollection = new List<KeyValuePair<string, string>>();
        string viewerVariableName = targetUser + userId + eventSource + "RankSystem";
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) == null)
            InitializeUserGlobalVar(viewerVariableName, eventSource);

        string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
        userRankCollection = new List<KeyValuePair<string, string>>();
        userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
        string keyToUpdate = "Coins";
        int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToUpdate);
        
        if (index == -1)
            userRankCollection.Add(new KeyValuePair<string, string>(keyToUpdate, coinsToAdd.ToString()));

        if (index != -1)
        {
            string newValue = (int.Parse(userRankCollection[index].Value) + coinsToAdd).ToString();
            userRankCollection[index] = new KeyValuePair<string, string>(keyToUpdate, newValue);
        }

        CPH.SetGlobalVar(viewerVariableName, JsonConvert.SerializeObject(userRankCollection), true);
        return true;
    }

    public bool AddCoins(){

        if (!CPH.TryGetArg("userName", out string userName))
            return false;

       userName = userName.ToLower();
        
        if (!CPH.TryGetArg("userId", out string userId))
            if (!CPH.TryGetArg("minichat.Data.UserID", out userId))
                return false;

        if (!CPH.TryGetArg("eventSource", out string eventSource))
            if (!CPH.TryGetArg("commandSource", out eventSource))
                return false;
        
        if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd))
            return false;

        AddCoins(coinsToAdd, eventSource, userName, userId);

        return true;
    }

    public bool GetWatchTime()
    {
        if (!CPH.TryGetArg("commandSource", out string commandSource))
            return false;

        if (commandSource.ToLower().Equals("vkplay"))
            commandSource = "vkvideolive";

        if (!CPH.TryGetArg("userName", out string userName))
            return false;

        string userId = "";
        
        if (!CPH.TryGetArg("userId", out userId))
            CPH.TryGetArg("minichat.Data.UserID", out userId);

        string viewerVariableName = userName.ToLower() + userId + commandSource.ToLower() + "RankSystem";

        string message = "Запрошенная информация не найдена!";
        var userRankCollection = new List<KeyValuePair<string, string>>();
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) != null)
        {
            string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
            userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
            string keyToShow = "WatchTime";
            int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToShow);
            if (index != -1) {
                long userWatchTime = long.Parse(userRankCollection[index].Value);
                message = FormatDateTime(userWatchTime);
            }
        }
        SendReply(message, commandSource.ToLower());
        return true;
    }

    public bool GetCoins()
    {
        if (!CPH.TryGetArg("commandSource", out string commandSource))
            return false;
        
        if (commandSource.ToLower().Equals("vkplay"))
            commandSource = "vkvideolive";

        if (!CPH.TryGetArg("userName", out string userName))
            return false;

        string userId = "";
        
        if (!CPH.TryGetArg("userId", out userId))
            CPH.TryGetArg("minichat.Data.UserID", out userId);

        string viewerVariableName = userName.ToLower() + userId + commandSource.ToLower() + "RankSystem";
        string message = "Запрошенная информация не найдена!";
        var userRankCollection = new List<KeyValuePair<string, string>>();
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) != null)
        {
            string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
            userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
            string keyToShow = "Coins";
            int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToShow);
            if (index != -1) {
                message = userRankCollection[index].Value.ToString();         
            }
        }
        SendReply(message, commandSource.ToLower());
        return true;
    }

    public bool GetGameWhenFollow()
    {
        if (!CPH.TryGetArg("commandSource", out string commandSource))
            return false;
        
        if (commandSource.ToLower().Equals("vkplay"))
            commandSource = "vkvideolive";

        if (!CPH.TryGetArg("userName", out string userName))
            return false;

        string userId = "";
        
        if (!CPH.TryGetArg("userId", out userId))
            CPH.TryGetArg("minichat.Data.UserID", out userId);

        string viewerVariableName = userName.ToLower() + userId + commandSource.ToLower() + "RankSystem";
        string message = "Запрошенная информация не найдена!";
        var userRankCollection = new List<KeyValuePair<string, string>>();
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) != null)
        {
            string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
            userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
            string keyToShow = "GameWhenFollow";
            int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToShow);
            if (index != -1) {
                message = userRankCollection[index].Value.ToString();         
            }
        }
        SendReply(message, commandSource.ToLower());
        return true;
    }

    private void AddGameWhenFollow(string gameToAdd, string eventSource, string targetUser, string userId){
        
        var userRankCollection = new List<KeyValuePair<string, string>>();
        string viewerVariableName = targetUser + userId + eventSource + "RankSystem";
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) == null)
            InitializeUserGlobalVar(viewerVariableName, eventSource);

        string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
        userRankCollection = new List<KeyValuePair<string, string>>();
        userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
        string keyToUpdate = "GameWhenFollow";
        int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToUpdate);
        
        if (index == -1)
            userRankCollection.Add(new KeyValuePair<string, string>(keyToUpdate, gameToAdd.ToString()));

        if (index != -1)
        {
            userRankCollection[index] = new KeyValuePair<string, string>(keyToUpdate, gameToAdd);
        }

        CPH.SetGlobalVar(viewerVariableName, JsonConvert.SerializeObject(userRankCollection), true);
    }

    private bool SendReply(string message, string target){

        if (target.Equals("twitch"))
            CPH.SendMessage(message);

        else if (target.Equals("youtube"))
            CPH.SendYouTubeMessage(message);

        else if (target.Equals("trovo"))
            CPH.SendTrovoMessage(message);
        
        else {
            CPH.SetArgument("message", message);
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

        if (days >= 30) // Примерно считаем месяцы
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
            result += $"{seconds.ToString()} {GetSecondWord(seconds)} ";

        return result.Trim(); // Убираем лишние пробелы в конце
    }

    static string GetYearWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11) return "год";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20)) return "года";
        return "лет";
    }

    static string GetMonthWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11) return "месяц";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20)) return "месяца";
        return "месяцев";
    }

    static string GetDayWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11) return "день";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20)) return "дня";
        return "дней";
    }

    static string GetHourWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11) return "час";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20)) return "часа";
        return "часов";
    }

    static string GetMinuteWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11) return "минуту";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20)) return "минуты";
        return "минут";
    }

    static string GetSecondWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11) return "секунду";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20)) return "секунды";
        return "секунд";
    }
}