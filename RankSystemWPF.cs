///----------------------------------------------------------------------------
///   Module:       RankSystem WPF
///   Author:       NuboHeimer (https://live.vkvideo.ru/nuboheimer)
///   Email:        nuboheimer@yandex.ru
///   Help:         https://t.me/nuboheimersb/5
///----------------------------------------------------------------------------

///   Version:      0.11.0 (WPF Version)

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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.ComponentModel;
using System.Collections.ObjectModel;
using System.Windows.Threading;

public class CPHInline
{
    private const int DEFAULT_TIME_TO_ADD = 60; // по умолчанию мы добавляем 60 секунд к времени просмотра.
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

    public bool AddCoins()
    {
        try
        {
            string service = RankSystemInternal.NormalizeService(this);
            var user = CreateUserFormArgs(service);
            var existingUser = DatabaseManager.GetUserData(filter: "Service = @Service AND ServiceUserId = @ServiceUserId", parameters: new[] { new SQLiteParameter("@Service", user.Service), new SQLiteParameter("@ServiceUserId", user.ServiceUserId) }).FirstOrDefault();
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
            long coinsFromArgs = DEFAULT_COINS_TO_ADD;
            if (!CPH.TryGetArg("coinsToAdd", out coinsFromArgs))
                coinsFromArgs = DEFAULT_COINS_TO_ADD;
            user.Coins += coinsFromArgs;
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
                    CPH.SetArgument("coinsToAdd", -actionCurrency);
                    AddCoins();
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
        if (string.IsNullOrEmpty(userName))
        {
            if (args.ContainsKey("userName"))
            {
                userName = args["userName"].ToString().ToLower();
            }
            else if (args.ContainsKey("users"))
            {
                // Пытаемся получить имя из списка пользователей (для случаев с одним пользователем)
                try
                {
                    var usersList = args["users"] as List<Dictionary<string, object>>;
                    if (usersList != null && usersList.Count > 0 && usersList[0].ContainsKey("userName"))
                    {
                        userName = usersList[0]["userName"].ToString().ToLower();
                    }
                }
                catch (Exception ex)
                {
                    CPH.LogWarn($"[RankSystem] Failed to extract userName from users list: {ex.Message}");
                }
            }
        }

        // Если userName все еще null, используем временное имя
        if (string.IsNullOrEmpty(userName))
        {
            CPH.LogWarn($"[RankSystem] UserName is NULL or empty for service {service}, userId {serviceUserId}. Using temporary name.");
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
        try
        {
            var app = new RankSystemWPFApp();
            app.Run();
            return true;
        }
        catch (Exception ex)
        {
            CPH.LogError($"[RankSystem] Failed to open WPF editor: {ex}");
            return false;
        }
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

public class UserData : INotifyPropertyChanged
{
    private string _uuid;
    private string _service;
    private string _serviceUserId;
    private string _userName;
    private long _watchTime;
    private DateTime _followDate;
    private long _messageCount;
    private long _coins;
    private string _gameWhenFollow;

    public string UUID
    {
        get => _uuid;
        set
        {
            _uuid = value;
            OnPropertyChanged(nameof(UUID));
        }
    }

    public string Service
    {
        get => _service;
        set
        {
            _service = value;
            OnPropertyChanged(nameof(Service));
        }
    }

    public string ServiceUserId
    {
        get => _serviceUserId;
        set
        {
            _serviceUserId = value;
            OnPropertyChanged(nameof(ServiceUserId));
        }
    }

    public string UserName
    {
        get => _userName;
        set
        {
            _userName = value;
            OnPropertyChanged(nameof(UserName));
        }
    }

    public long WatchTime
    {
        get => _watchTime;
        set
        {
            _watchTime = value;
            OnPropertyChanged(nameof(WatchTime));
        }
    }

    public DateTime FollowDate
    {
        get => _followDate;
        set
        {
            _followDate = value;
            OnPropertyChanged(nameof(FollowDate));
        }
    }

    public long MessageCount
    {
        get => _messageCount;
        set
        {
            _messageCount = value;
            OnPropertyChanged(nameof(MessageCount));
        }
    }

    public long Coins
    {
        get => _coins;
        set
        {
            _coins = value;
            OnPropertyChanged(nameof(Coins));
        }
    }

    public string GameWhenFollow
    {
        get => _gameWhenFollow;
        set
        {
            _gameWhenFollow = value;
            OnPropertyChanged(nameof(GameWhenFollow));
        }
    }

    public event PropertyChangedEventHandler PropertyChanged;

    protected virtual void OnPropertyChanged(string propertyName)
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}

public class UserNameHistory
{
    public long Id { get; set; }
    public string UUID { get; set; }
    public string OldUserName { get; set; }
    public string NewUserName { get; set; }
    public DateTime ChangeDate { get; set; }
}

// WPF Application
public class RankSystemWPFApp : Application
{
    protected override void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);

        var mainWindow = new RankSystemMainWindow();
        mainWindow.Show();
        MainWindow = mainWindow;
    }
}

// Main Window
public class RankSystemMainWindow : Window
{
    private DataGrid usersDataGrid;
    private TextBox searchBox;
    private Button addButton;
    private Button refreshButton;
    private Button clearCoinsButton;
    private Button dropDatabaseButton;
    private UserDataViewModel viewModel;

    public RankSystemMainWindow()
    {
        InitializeComponent();
        viewModel = new UserDataViewModel();
        DataContext = viewModel;

        // Загружаем данные при запуске
        viewModel.LoadUsers();
    }

    private void InitializeComponent()
    {
        Title = "RankSystem - Система рангов для стриминга";
        Width = 1400;
        Height = 800;
        WindowStartupLocation = WindowStartupLocation.CenterScreen;
        WindowState = WindowState.Maximized;

        // Создаем основной контейнер
        var mainGrid = new Grid();
        mainGrid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
        mainGrid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
        mainGrid.RowDefinitions.Add(new RowDefinition { Height = new GridLength(1, GridUnitType.Star) });
        mainGrid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

        // Заголовок
        var headerPanel = new StackPanel
        {
            Orientation = Orientation.Horizontal,
            Margin = new Thickness(20, 20, 20, 10),
            Background = new SolidColorBrush(Color.FromRgb(33, 150, 243))
        };

        var titleText = new TextBlock
        {
            Text = "🎯 RankSystem - Система рангов для стриминга",
            FontSize = 24,
            FontWeight = FontWeights.Bold,
            Foreground = Brushes.White,
            VerticalAlignment = VerticalAlignment.Center
        };

        headerPanel.Children.Add(titleText);
        Grid.SetRow(headerPanel, 0);

        // Панель поиска и кнопок
        var controlPanel = new Grid();
        controlPanel.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
        controlPanel.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
        controlPanel.Margin = new Thickness(20, 10, 20, 10);

        // Поиск
        var searchPanel = new StackPanel { Orientation = Orientation.Horizontal };
        var searchLabel = new Label { Content = "🔍 Поиск:", VerticalAlignment = VerticalAlignment.Center };
        searchBox = new TextBox
        {
            Width = 300,
            Height = 30,
            FontSize = 14,
            VerticalAlignment = VerticalAlignment.Center
        };
        searchBox.TextChanged += SearchBox_TextChanged;
        searchPanel.Children.Add(searchLabel);
        searchPanel.Children.Add(searchBox);

        // Кнопки
        var buttonPanel = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = HorizontalAlignment.Right };

        addButton = CreateStyledButton("➕ Добавить", Colors.Green);
        addButton.Click += AddButton_Click;

        refreshButton = CreateStyledButton("🔄 Обновить", Colors.Blue);
        refreshButton.Click += RefreshButton_Click;

        clearCoinsButton = CreateStyledButton("💰 Очистить коины", Colors.Orange);
        clearCoinsButton.Click += ClearCoinsButton_Click;

        dropDatabaseButton = CreateStyledButton("🗑️ Удалить БД", Colors.Red);
        dropDatabaseButton.Click += DropDatabaseButton_Click;

        buttonPanel.Children.Add(addButton);
        buttonPanel.Children.Add(refreshButton);
        buttonPanel.Children.Add(clearCoinsButton);
        buttonPanel.Children.Add(dropDatabaseButton);

        Grid.SetColumn(searchPanel, 0);
        Grid.SetColumn(buttonPanel, 1);
        controlPanel.Children.Add(searchPanel);
        controlPanel.Children.Add(buttonPanel);
        Grid.SetRow(controlPanel, 1);

        // DataGrid
        usersDataGrid = new DataGrid
        {
            AutoGenerateColumns = false,
            CanUserAddRows = false,
            CanUserDeleteRows = false,
            SelectionMode = DataGridSelectionMode.Single,
            GridLinesVisibility = DataGridGridLinesVisibility.Horizontal,
            HeadersVisibility = DataGridHeadersVisibility.Column,
            RowHeaderWidth = 0,
            Margin = new Thickness(20, 10, 20, 10)
        };

        // Настройка колонок
        usersDataGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "UUID",
            Binding = new Binding("UUID"),
            Width = 120,
            IsReadOnly = true
        });

        usersDataGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "Сервис",
            Binding = new Binding("Service"),
            Width = 80,
            IsReadOnly = true
        });

        usersDataGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "ID пользователя",
            Binding = new Binding("ServiceUserId"),
            Width = 120,
            IsReadOnly = true
        });

        usersDataGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "Имя пользователя",
            Binding = new Binding("UserName"),
            Width = 120,
            IsReadOnly = false
        });

        usersDataGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "Время просмотра",
            Binding = new Binding("WatchTime"),
            Width = 100,
            IsReadOnly = false
        });

        usersDataGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "Дата фоллоу",
            Binding = new Binding("FollowDate") { StringFormat = "dd.MM.yyyy HH:mm" },
            Width = 120,
            IsReadOnly = false
        });

        usersDataGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "Сообщения",
            Binding = new Binding("MessageCount"),
            Width = 80,
            IsReadOnly = false
        });

        usersDataGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "Монеты",
            Binding = new Binding("Coins"),
            Width = 80,
            IsReadOnly = false
        });

        usersDataGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "Игра при фоллоу",
            Binding = new Binding("GameWhenFollow"),
            Width = 120,
            IsReadOnly = false
        });

        usersDataGrid.CellEditEnding += UsersDataGrid_CellEditEnding;
        usersDataGrid.SelectionChanged += UsersDataGrid_SelectionChanged;
        usersDataGrid.ItemsSource = viewModel.FilteredUsers;
        Grid.SetRow(usersDataGrid, 2);

        // Статус бар
        var statusBar = new System.Windows.Controls.Primitives.StatusBar();
        var statusText = new TextBlock { Text = "Готов к работе" };
        statusBar.Items.Add(statusText);
        Grid.SetRow(statusBar, 3);

        // Добавляем элементы в основной грид
        mainGrid.Children.Add(headerPanel);
        mainGrid.Children.Add(controlPanel);
        mainGrid.Children.Add(usersDataGrid);
        mainGrid.Children.Add(statusBar);

        Content = mainGrid;
    }

    private Button CreateStyledButton(string text, Color color)
    {
        return new Button
        {
            Content = text,
            Width = 120,
            Height = 35,
            Margin = new Thickness(5),
            Background = new SolidColorBrush(color),
            Foreground = Brushes.White,
            FontWeight = FontWeights.Bold,
            BorderThickness = new Thickness(0),
            Cursor = Cursors.Hand
        };
    }

    private void SearchBox_TextChanged(object sender, TextChangedEventArgs e)
    {
        viewModel.FilterUsers(searchBox.Text);
    }

    private void AddButton_Click(object sender, RoutedEventArgs e)
    {
        viewModel.AddNewUser();
        usersDataGrid.SelectedIndex = 0;
    }

    private void RefreshButton_Click(object sender, RoutedEventArgs e)
    {
        viewModel.LoadUsers();
        searchBox.Text = "";
    }

    private void ClearCoinsButton_Click(object sender, RoutedEventArgs e)
    {
        var result = MessageBox.Show(
            "Вы уверены, что хотите очистить коины у всех пользователей?",
            "Подтверждение",
            MessageBoxButton.YesNo,
            MessageBoxImage.Warning);

        if (result == MessageBoxResult.Yes)
        {
            viewModel.ClearAllCoins();
        }
    }

    private void DropDatabaseButton_Click(object sender, RoutedEventArgs e)
    {
        var result = MessageBox.Show(
            "⚠️ ВНИМАНИЕ! Это действие полностью удалит базу данных!\n\nВы уверены, что хотите продолжить?",
            "Критическое действие",
            MessageBoxButton.YesNo,
            MessageBoxImage.Error);

        if (result == MessageBoxResult.Yes)
        {
            viewModel.DropDatabase();
        }
    }

    private void UsersDataGrid_CellEditEnding(object sender, DataGridCellEditEndingEventArgs e)
    {
        if (e.EditAction == DataGridEditAction.Commit)
        {
            var user = e.Row.Item as UserData;
            if (user != null)
            {
                viewModel.UpdateUser(user);
            }
        }
    }

    private void UsersDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        // Можно добавить дополнительную логику при выборе пользователя
    }
}

// ViewModel для управления данными
public class UserDataViewModel : INotifyPropertyChanged
{
    private ObservableCollection<UserData> _users;
    private ObservableCollection<UserData> _filteredUsers;
    private string _searchText;
    private UserData _selectedUser;

    public ObservableCollection<UserData> Users
    {
        get => _users;
        set
        {
            _users = value;
            OnPropertyChanged(nameof(Users));
        }
    }

    public ObservableCollection<UserData> FilteredUsers
    {
        get => _filteredUsers;
        set
        {
            _filteredUsers = value;
            OnPropertyChanged(nameof(FilteredUsers));
        }
    }

    public string SearchText
    {
        get => _searchText;
        set
        {
            _searchText = value;
            OnPropertyChanged(nameof(SearchText));
            FilterUsers(value);
        }
    }

    public UserData SelectedUser
    {
        get => _selectedUser;
        set
        {
            _selectedUser = value;
            OnPropertyChanged(nameof(SelectedUser));
        }
    }

    public UserDataViewModel()
    {
        Users = new ObservableCollection<UserData>();
        FilteredUsers = new ObservableCollection<UserData>();
    }

    public void LoadUsers()
    {
        try
        {
            var users = DatabaseManager.GetUserData();
            Users.Clear();
            foreach (var user in users)
            {
                Users.Add(user);
            }
            FilteredUsers = new ObservableCollection<UserData>(Users);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Ошибка загрузки данных: {ex.Message}", "Ошибка", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    public void FilterUsers(string searchText)
    {
        if (string.IsNullOrWhiteSpace(searchText))
        {
            FilteredUsers = new ObservableCollection<UserData>(Users);
        }
        else
        {
            var filtered = Users.Where(user =>
                user.UserName?.Contains(searchText, StringComparison.OrdinalIgnoreCase) == true ||
                user.Service?.Contains(searchText, StringComparison.OrdinalIgnoreCase) == true ||
                user.ServiceUserId?.Contains(searchText, StringComparison.OrdinalIgnoreCase) == true ||
                user.UUID?.Contains(searchText, StringComparison.OrdinalIgnoreCase) == true
            ).ToList();

            FilteredUsers = new ObservableCollection<UserData>(filtered);
        }
    }

    public void AddNewUser()
    {
        var newUser = new UserData
        {
            UUID = Guid.NewGuid().ToString(),
            Service = "",
            ServiceUserId = "",
            UserName = "",
            WatchTime = 0,
            FollowDate = DateTime.MinValue,
            MessageCount = 0,
            Coins = 0,
            GameWhenFollow = ""
        };

        Users.Insert(0, newUser);
        FilteredUsers.Insert(0, newUser);
        DatabaseManager.UpsertUser(newUser);
    }

    public void UpdateUser(UserData user)
    {
        try
        {
            DatabaseManager.UpsertUser(user);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Ошибка обновления пользователя: {ex.Message}", "Ошибка", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    public void ClearAllCoins()
    {
        try
        {
            DatabaseManager.ClearAllUsersCoins();
            LoadUsers();
            MessageBox.Show("Коины всех пользователей очищены.", "Успех", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Ошибка очистки коинов: {ex.Message}", "Ошибка", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    public void DropDatabase()
    {
        try
        {
            DatabaseManager.DropDatabase();
            LoadUsers();
            MessageBox.Show("База данных полностью очищена.", "Успех", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Ошибка очистки базы данных: {ex.Message}", "Ошибка", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    public event PropertyChangedEventHandler PropertyChanged;

    protected virtual void OnPropertyChanged(string propertyName)
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}

// Расширение для String.Contains с поддержкой StringComparison
public static class StringExtensions
{
    public static bool Contains(this string source, string value, StringComparison comparisonType)
    {
        return source?.IndexOf(value, comparisonType) >= 0;
    }
}

// Метод для очистки коинов всех пользователей
public static class DatabaseManagerExtensions
{
    public static void ClearAllUsersCoins()
    {
        var users = DatabaseManager.GetUserData();
        foreach (var user in users)
        {
            user.Coins = 0;
            DatabaseManager.UpsertUser(user);
        }
    }
}

// Менеджер базы данных
public static class DatabaseManager
{
    private static readonly string DbPath = "RankSystem.db";
    private static readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
    private static readonly ReaderWriterLockSlim _historyLock = new ReaderWriterLockSlim();

    private static SQLiteConnection CreateConnection()
    {
        return new SQLiteConnection($"Data Source={DbPath};Version=3;");
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
                    // Создаем таблицу пользователей
                    cmd.CommandText = @"
                    CREATE TABLE IF NOT EXISTS Users (
                        UUID TEXT PRIMARY KEY,
                        Service TEXT NOT NULL,
                        ServiceUserId TEXT NOT NULL,
                        UserName TEXT NOT NULL,
                        WatchTime INTEGER DEFAULT 0,
                        FollowDate TEXT DEFAULT '',
                        MessageCount INTEGER DEFAULT 0,
                        Coins INTEGER DEFAULT 0,
                        GameWhenFollow TEXT,
                        UNIQUE(Service, ServiceUserId)
                    )";
                    cmd.ExecuteNonQuery();

                    // Создаем таблицу истории изменений имен пользователей
                    cmd.CommandText = @"
                    CREATE TABLE IF NOT EXISTS UserNameHistory (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        UUID TEXT NOT NULL,
                        OldUserName TEXT NOT NULL,
                        NewUserName TEXT NOT NULL,
                        ChangeDate TEXT NOT NULL,
                        FOREIGN KEY(UUID) REFERENCES Users(UUID)
                    )";
                    cmd.ExecuteNonQuery();

                    // Создаем индексы для улучшения производительности
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_users_service_userid ON Users(Service, ServiceUserId)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_users_username ON Users(UserName)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_users_watchtime ON Users(WatchTime)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_users_messagecount ON Users(MessageCount)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_users_coins ON Users(Coins)";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_usernamehistory_uuid ON UserNameHistory(UUID)";
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
            if (File.Exists(DbPath))
            {
                File.Delete(DbPath);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public static void UpsertUser(UserData user)
    {
        _lock.EnterWriteLock();
        string uuid = null;
        string oldUserName = null;
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
                            // Проверяем, существует ли пользователь
                            cmd.CommandText = "SELECT UUID, UserName FROM Users WHERE Service = @Service AND ServiceUserId = @ServiceUserId";
                            cmd.Parameters.AddWithValue("@Service", user.Service);
                            cmd.Parameters.AddWithValue("@ServiceUserId", user.ServiceUserId);
                            using (var reader = cmd.ExecuteReader())
                            {
                                if (reader.Read())
                                {
                                    uuid = reader["UUID"].ToString();
                                    oldUserName = reader["UserName"].ToString();
                                }
                            }

                            // Обновляем или вставляем пользователя
                            cmd.CommandText = @"
                                INSERT OR REPLACE INTO Users (
                                    UUID, Service, ServiceUserId, UserName, WatchTime, 
                                    FollowDate, MessageCount, Coins, GameWhenFollow
                                ) VALUES (
                                    @UUID, @Service, @ServiceUserId, @UserName, @WatchTime,
                                    @FollowDate, @MessageCount, @Coins, @GameWhenFollow
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

    public static void ClearAllUsersCoins()
    {
        _lock.EnterWriteLock();
        try
        {
            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var cmd = new SQLiteCommand(connection))
                {
                    cmd.CommandText = "UPDATE Users SET Coins = 0";
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

// Внутренняя логика системы рангов
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

        return userData?.GameWhenFollow ?? "";
    }

    public static string NormalizeService(CPHInline cph)
    {
        if (cph.args.ContainsKey("eventSource"))
        {
            string eventSource = cph.args["eventSource"].ToString().ToLower();
            if (eventSource.Contains("twitch"))
                return "twitch";
            else if (eventSource.Contains("kick"))
                return "Kick";
            else if (eventSource.Contains("trovo"))
                return "trovo";
            else if (eventSource.Contains("minichat"))
                return "vkvideolive";
        }

        // Fallback для случаев, когда eventSource не определен
        return "vkvideolive";
    }

    public static string FormatValue(UserData user, string field)
    {
        return field.ToLower() switch
        {
            "watchtime" => FormatDateTime(user.WatchTime),
            "messagecount" => user.MessageCount.ToString(),
            "coins" => user.Coins.ToString(),
            _ => "Unknown field"
        };
    }

    public static string FormatDateTime(long totalSeconds)
    {
        if (totalSeconds <= 0)
            return "0 секунд";

        var timeSpan = TimeSpan.FromSeconds(totalSeconds);
        var parts = new List<string>();

        if (timeSpan.Days > 0)
            parts.Add($"{timeSpan.Days} {GetDayWord(timeSpan.Days)}");
        if (timeSpan.Hours > 0)
            parts.Add($"{timeSpan.Hours} {GetHourWord(timeSpan.Hours)}");
        if (timeSpan.Minutes > 0)
            parts.Add($"{timeSpan.Minutes} {GetMinuteWord(timeSpan.Minutes)}");
        if (timeSpan.Seconds > 0)
            parts.Add($"{timeSpan.Seconds} {GetSecondWord(timeSpan.Seconds)}");

        return string.Join(" ", parts);
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
            return "минута";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "минуты";
        return "минут";
    }

    static string GetSecondWord(int count)
    {
        if (count % 10 == 1 && count % 100 != 11)
            return "секунда";
        if (count % 10 >= 2 && count % 10 <= 4 && (count % 100 < 10 || count % 100 >= 20))
            return "секунды";
        return "секунд";
    }
}
