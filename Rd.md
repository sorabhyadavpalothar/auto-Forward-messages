telegram-bot/                  # project root
├── README.md
├── .env.example
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── bot/                        # main python package
│   ├── __init__.py
│   ├── config.py               # configuration loader (env / constants)
│   ├── bot.py                  # entrypoint: create client & register handlers
│   ├── scheduler.py            # scheduler for scheduled forwards
│   ├── db.py                   # database helpers (sqlite / postgres)
│   ├── utils.py                # small helper functions
│   ├── handlers/               # all telegram event handlers
│   │   ├── __init__.py
│   │   ├── auth_handler.py
│   │   ├── message_handler.py
│   │   ├── forwarder.py
│   │   └── admin_commands.py
│   ├── services/               # business logic, separated from handlers
│   │   ├── __init__.py
│   │   ├── forward_service.py
│   │   └── user_service.py
│   ├── models/                 # db models / schemas
│   │   ├── __init__.py
│   │   └── user.py
│   └── templates/              # optional: message templates / HTML
│       └── welcome.txt
│
├── scripts/                    # helper scripts (migrations, cron jobs)
│   ├── migrate.py
│   └── seed.py
│
├── migrations/                 # alembic / sqlite migration files (optional)
│
├── tests/                      # unit / integration tests
│   ├── conftest.py
│   └── test_forward.py
│
└── docs/                       # architecture docs, API references
    └── architecture.md