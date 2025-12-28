# Database Overview

The files in this folder create the database based on which our analysis is conducted. The database structure is illustrated in the following image:

![Database Structure](99_misc/Database_Structure.png)

## How-To

First, create the _leagues_ table with the pre-defined leagues in `database/input/leagues.yaml`: 

```
python -m database.01_leagues
```

```
├── dir1
│   ├── file11.ext
│   └── file12.ext
├── dir2
│   ├── file21.ext
│   ├── file22.ext
│   └── file23.ext
├── dir3
├── file_in_root.ext
└── README.md
```