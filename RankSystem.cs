///----------------------------------------------------------------------------
///   Module:       RankSystem
///   Author:       NuboHeimer (https://live.vkvideo.ru/nuboheimer)
///   Email:        nuboheimer@yandex.ru
///----------------------------------------------------------------------------
 
///   Version:      0.0.1
using System;
using System.Collections.Generic;
using Newtonsoft.Json;

public class CPHInline
{
    private const int DEFAULT_TIME_TO_ADD = 60; // по умолчанию мы добавляем 60 секунд к времени просмотра.
    private bool InitializeUserGlobalVar(string viewerVariableName, string eventSource)
    {
        var userRankCollection = new List<KeyValuePair<string, string>>();
        userRankCollection.Add(new KeyValuePair<string, string>(eventSource + "WatchTime", "0"));
        userRankCollection.Add(new KeyValuePair<string, string>(eventSource + "MessageCount", "0"));
        userRankCollection.Add(new KeyValuePair<string, string>(eventSource + "FollowDate", null));
        userRankCollection.Add(new KeyValuePair<string, string>(eventSource + "Rank", null));

        CPH.SetGlobalVar(viewerVariableName, JsonConvert.SerializeObject(userRankCollection), true);

        return true;
    }

    public bool AddWatchTime()
    {
        string eventSource = "Test";
        var userRankCollection = new List<KeyValuePair<string, string>>();

        if (args["eventSource"].ToString().Equals("misc")) // если в eventSource лежит misc -- значит это не дефолтный PresentVieewr.
        {
            if (args.ContainsKey("timerId")) // если это кастомный таймер -- надо понять, какой именно.
            {
                // Возможно, стоит переделать на id || name.
                if (args["timerId"].ToString().Equals("1da45ce2-2383-4431-8b42-b4f3314d2d79"))
                    eventSource = "VKVideoLive";
            }
        }
        else
            eventSource = args["eventSource"].ToString(); // иначе просто берём источник события (twitch/youtube/trovo).

        if (args.ContainsKey("users")) // защита от дурака с пустым аргументом списка пользователей.
        {
            var currentViewers = (List<Dictionary<string, object>>)args["users"];

            if (currentViewers.Count == 0)
                return true; // выходим, если список зрителей всё же пустой.

            if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd)) // записываем значение валюты за минуты просмотра, если она задана в настройках экшена
                coinsToAdd = 0; // или ставим её в ноль.

            if (!CPH.TryGetArg("timeToAdd", out int timeToAdd)) // записываем, сколько добавлять времени, если это задано в настройках экшене
                timeToAdd = DEFAULT_TIME_TO_ADD; // или записываем дефолтное значение

            foreach (var viewer in currentViewers) // проходимся по зрителям в списке.
            {
                string viewerName = viewer["userName"].ToString().ToLower();

                if (CPH.TryGetArg("viewersBlackList", out string tempViewersBlackList)) // проверяем чёрный список зрителей.
                {
                    List<string> viewersBlackList = new List<string>(tempViewersBlackList.ToLower().Split(';'));
                    if (viewersBlackList.Contains(viewerName))
                        continue; // пропускаем итерацию если существует чёрный список зрителей и текущий в нём есть.
                }

                string viewerVariableName = viewerName + "RankSystem";

                if (CPH.GetGlobalVar<string>(viewerVariableName, true) == null) // если для текущего зрителя ещё нет глобальной переменной -- инициализируем её.
                    InitializeUserGlobalVar(viewerVariableName, eventSource);

                string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
                userRankCollection = new List<KeyValuePair<string, string>>();
                userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
                string keyToUpdate = eventSource + "WatchTime";
                int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToUpdate);
                
                if (index == -1)
                    /*  TODO Добавь еще триггер старта сб
                        Прожми тест триггер на него, в аргументах найдешь признак того, что запустилось по старту сб
                        Добавляешь код, в нем проверку на триггер. Если старт сб - запомни время
                        Иначе - возьми дельту (с) Play_Code.
                    */
                    userRankCollection.Add(new KeyValuePair<string, string>(keyToUpdate, timeToAdd.ToString())); // если у пользователя ещё нет времени просмотра задаём начальное значение.
                else
                {
                    string newValue = (int.Parse(userRankCollection[index].Value) + timeToAdd).ToString(); // добавляем время просмотра
                    userRankCollection[index] = new KeyValuePair<string, string>(keyToUpdate, newValue);
                }

                CPH.SetGlobalVar(viewerVariableName, JsonConvert.SerializeObject(userRankCollection), true);
                if (coinsToAdd > 0) // если указано количество валюты для добавления за время просмотра
                    AddCoins(coinsToAdd, eventSource, viewerName);
            }
        }

        return true;
    }

    public bool AddMessageCount()
    {
        var userRankCollection = new List<KeyValuePair<string, string>>();
        string eventSource = args["eventSource"].ToString();
        string userName = args["userName"].ToString().ToLower();
        if (CPH.TryGetArg("viewersBlackList", out string tempViewersBlackList))
        {
            List<string> viewersBlackList = new List<string>(tempViewersBlackList.ToLower().Split(';'));
            if (viewersBlackList.Contains(userName))
                return false;
        }

        if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd)) // записываем значение валюты за сообщение, если она задана в настройках экшена
            coinsToAdd = 0; // или ставим её в ноль.

        string viewerVariableName = userName + "RankSystem";
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) == null)
            InitializeUserGlobalVar(viewerVariableName, eventSource);

        string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
        userRankCollection = new List<KeyValuePair<string, string>>();
        userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
        string keyToUpdate = eventSource + "MessageCount";
        int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToUpdate);
        
        if (index == -1)
            userRankCollection.Add(new KeyValuePair<string, string>(keyToUpdate, "1"));

        if (index != -1)
        {
            string newValue = (int.Parse(userRankCollection[index].Value) + 1).ToString();
            userRankCollection[index] = new KeyValuePair<string, string>(keyToUpdate, newValue);
            
        }

        CPH.SetGlobalVar(viewerVariableName, JsonConvert.SerializeObject(userRankCollection), true);
        if (coinsToAdd > 0) // если указано количество валюты для добавления за сообщение
            AddCoins(coinsToAdd, eventSource, userName);

        return true;
    }

    public bool AddFollowDate()
    {
        var userRankCollection = new List<KeyValuePair<string, string>>();
        string eventSource = args["eventSource"].ToString();
        string userName = args["userName"].ToString().ToLower();
        string viewerVariableName = userName + "RankSystem";

        if (!CPH.TryGetArg("coinsToAdd", out int coinsToAdd)) // записываем значение валюты за фоллов, если она задана в настройках экшена
            coinsToAdd = 0; // или ставим её в ноль.
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) == null)
            InitializeUserGlobalVar(viewerVariableName, eventSource);

        string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
        userRankCollection = new List<KeyValuePair<string, string>>();
        userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
        string keyToUpdate = eventSource + "FollowDate";
        int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToUpdate);
        
        if (index == -1)
            userRankCollection.Add(new KeyValuePair<string, string>(keyToUpdate, DateTime.Now.ToString()));

        if (index != -1)
        {
            string newValue = DateTime.Now.ToString();
            userRankCollection[index] = new KeyValuePair<string, string>(keyToUpdate, newValue);
        }

        CPH.SetGlobalVar(viewerVariableName, JsonConvert.SerializeObject(userRankCollection), true);
        if (coinsToAdd > 0) // если указано количество валюты для добавления за фоллов
            AddCoins(coinsToAdd, eventSource, userName);
        return true;
    }

    private bool AddCoins(int coinsToAdd, string eventSource, string targetUser)
    {
        var userRankCollection = new List<KeyValuePair<string, string>>();
        string viewerVariableName = targetUser + "RankSystem";
        
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

    public bool GetWatchTime()
    {
        if (!CPH.TryGetArg("commandSource", out string commandSource))
            return false;

        if (!CPH.TryGetArg("userName", out string userName))
            return false;

        string viewerVariableName = userName.ToLower() + "RankSystem";
        var userRankCollection = new List<KeyValuePair<string, string>>();
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) != null)
        {
            string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
            userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
            string keyToShow = commandSource + "WatchTime";
            int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToShow);
            if (index != -1) {
                long userWatchTime = long.Parse(userRankCollection[index].Value);
                // TODO REFACTOR
                // Определяем количество секунд в различных временных единицах
                const long secondsInMinute = 60;
                const long secondsInHour = 3600; // 60 * 60
                const long secondsInDay = 86400; // 24 * 60 * 60
                const long secondsInMonth = 2592000; // 30 * 24 * 60 * 60 (приблизительно)
                const long secondsInYear = 31536000; // 365 * 24 * 60 * 60 (приблизительно)

                // Вычисляем количество лет, месяцев, дней, часов, минут и секунд
                long years = userWatchTime / secondsInYear;
                userWatchTime %= secondsInYear;

                long months = userWatchTime / secondsInMonth;
                userWatchTime %= secondsInMonth;

                long days = userWatchTime / secondsInDay;
                userWatchTime %= secondsInDay;

                long hours = userWatchTime / secondsInHour;
                userWatchTime %= secondsInHour;

                long minutes = userWatchTime / secondsInMinute;
                long seconds = userWatchTime % secondsInMinute;

                // Формируем список частей времени
                var parts = new System.Collections.Generic.List<string>();

                if (years > 0)
                    parts.Add($"{years} лет{(years > 1 ? "" : "")}");

                if (months > 0)
                    parts.Add($"{months} месяцев{(months > 1 ? "" : "")}");

                if (days > 0)
                    parts.Add($"{days} дней{(days > 1 ? "" : "")}");

                if (hours > 0)
                    parts.Add($"{hours} часов{(hours > 1 ? "" : "")}");

                if (minutes > 0 || seconds > 0) // Добавляем минуты, если есть секунды
                    parts.Add($"{minutes} минут{(minutes > 1 ? "" : "")}");

                if (seconds > 0 || parts.Count == 0) // Добавляем секунды, если нет других частей
                    parts.Add($"{seconds} секунд{(seconds > 1 ? "" : "")}");


                // Объединяем части в строку
                CPH.SetArgument("userWatchTime", string.Join(" ", parts));
            }
            else
                CPH.SetArgument("userWatchTime", "Пользователь не найден!");
        }
        else
            CPH.SetArgument("userWatchTime", "Пользователь не найден!");
        return true;
    }

    public bool GetCoins()
    {
        if (!CPH.TryGetArg("commandSource", out string commandSource))
            return false;

        if (!CPH.TryGetArg("userName", out string userName))
            return false;

        string viewerVariableName = userName.ToLower() + "RankSystem";
        var userRankCollection = new List<KeyValuePair<string, string>>();
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) != null)
        {
            string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
            userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
            string keyToShow = "Coins";
            int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToShow);
            if (index != -1) {
                CPH.SetArgument("userCoins", userRankCollection[index].Value.ToString());                
            }
            else
                CPH.SetArgument("userCoins", "Пользователь не найден!");
        }
        else
            CPH.SetArgument("userCoins", "Пользователь не найден!");
        return true;
    }
}