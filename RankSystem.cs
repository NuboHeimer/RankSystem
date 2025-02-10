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
                    /*  TODO переделать на константу или аргумент.
                        Добавь еще триггер старта сб
                        Прожми тест триггер на него, в аргументах найдешь признак того, что запустилось по старту сб
                        Добавляешь код, в нем проверку на триггер. Если старт сб - запомни время
                        Иначе - возьми дельту (с) Play_Code.
                    */
                    userRankCollection.Add(new KeyValuePair<string, string>(keyToUpdate, "60")); // если у пользователя ещё нет времени просмотра задаём начальную минуту.
                else
                {
                    string newValue = (int.Parse(userRankCollection[index].Value) + 60).ToString(); // добавляем минуту к времени просмотра
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

        return true;
    }

    public bool AddFollowDate()
    {
        var userRankCollection = new List<KeyValuePair<string, string>>();
        string eventSource = args["eventSource"].ToString();
        string userName = args["userName"].ToString().ToLower();
        string viewerVariableName = userName + "RankSystem";
        
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

        string viewerVariableName = userName + "RankSystem";
        var userRankCollection = new List<KeyValuePair<string, string>>();
        
        if (CPH.GetGlobalVar<string>(viewerVariableName, true) != null)
        {
            string userRankInfo = CPH.GetGlobalVar<string>(viewerVariableName);
            userRankCollection = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(userRankInfo);
            string keyToShow = commandSource + "WatchTime";
            int index = userRankCollection.FindIndex(kvp => kvp.Key == keyToShow);
            if (index != -1)
                CPH.SetArgument("userWatchTime", userRankCollection[index].Value);
            else
                CPH.SetArgument("userWatchTime", "Пользователь не найден!");
        }
        else
            CPH.SetArgument("userWatchTime", "Пользователь не найден!");

        return true;
    }
}