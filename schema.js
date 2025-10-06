const sqlite3 = require('sqlite3').verbose();

const createSourceDatabase = () => {
    const db = new sqlite3.Database('./source.db', (err) => {
        if (err) return console.error('Error connecting to source DB:', err.message);
        console.log('Connected to the source SQLite database.');
    });

    db.serialize(() => {
        console.log('Creating source tables');
        db.run('DROP TABLE IF EXISTS users');
        db.run(`CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            full_name TEXT NOT NULL,
            email TEXT UNIQUE,
            join_date TEXT
        )`);

        db.run('DROP TABLE IF EXISTS content');
        db.run(`CREATE TABLE content (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            genre TEXT,
            release_year INTEGER,
            type TEXT
        )`);

        db.run('DROP TABLE IF EXISTS viewings');
        db.run(`CREATE TABLE viewings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            content_id INTEGER,
            view_date TEXT,
            duration_watched_minutes INTEGER,
            device TEXT
        )`);
        
        db.run('DROP TABLE IF EXISTS interactions');
        db.run(`CREATE TABLE interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            content_id INTEGER,
            interaction_date TEXT,
            interaction_type TEXT,
            rating_value INTEGER
        )`);

        db.run('DROP TABLE IF EXISTS subscriptions');
        db.run(`CREATE TABLE subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            event_date TEXT,
            plan_name TEXT,
            event_type TEXT,
            amount REAL
        )`);
    });

    db.close((err) => {
        if (err) return console.error('Error closing source DB:', err.message);
        console.log('Source database schema created and connection closed.');
    });
};

const createDestinationDatabase = () => {
    const db = new sqlite3.Database('./destination.db', (err) => {
        if (err) return console.error('Error connecting to destination DB:', err.message);
        console.log('Connected to the destination SQLite database.');
    });

    db.serialize(() => {
        console.log('Creating destination tables');
        
        db.run('DROP TABLE IF EXISTS DimUser');
        db.run('CREATE TABLE DimUser (user_id INTEGER PRIMARY KEY, fullName TEXT, email TEXT, joinDate TEXT)');
        
        db.run('DROP TABLE IF EXISTS DimContent');
        db.run('CREATE TABLE DimContent (content_id INTEGER PRIMARY KEY, title TEXT, type TEXT, mainGenre TEXT, releaseYear INTEGER)');

        db.run('DROP TABLE IF EXISTS DimDate');
        db.run('CREATE TABLE DimDate (date_id INTEGER PRIMARY KEY, fullDate TEXT, year INTEGER, quarter INTEGER, month INTEGER, dayOfWeek INTEGER)');

        db.run('DROP TABLE IF EXISTS DimSubscriptionPlan');
        db.run('CREATE TABLE DimSubscriptionPlan (plan_id INTEGER PRIMARY KEY AUTOINCREMENT, planName TEXT UNIQUE)');

        db.run('DROP TABLE IF EXISTS Fact_ViewingActivity');
        db.run(`CREATE TABLE Fact_ViewingActivity (
            viewing_id INTEGER PRIMARY KEY,
            date_id INTEGER,
            user_id INTEGER,
            content_id INTEGER,
            durationWatchedMinutes REAL,
            FOREIGN KEY(date_id) REFERENCES DimDate(date_id),
            FOREIGN KEY(user_id) REFERENCES DimUser(user_id),
            FOREIGN KEY(content_id) REFERENCES DimContent(content_id)
        )`);
        
        db.run('DROP TABLE IF EXISTS Fact_SubscriptionTransaction');
        db.run(`CREATE TABLE Fact_SubscriptionTransaction (
            transaction_id INTEGER PRIMARY KEY,
            date_id INTEGER,
            user_id INTEGER,
            plan_id INTEGER,
            transactionAmount REAL,
            FOREIGN KEY(date_id) REFERENCES DimDate(date_id),
            FOREIGN KEY(user_id) REFERENCES DimUser(user_id),
            FOREIGN KEY(plan_id) REFERENCES DimSubscriptionPlan(plan_id)
        )`);

        db.run('DROP TABLE IF EXISTS Fact_UserInteraction');
        db.run(`CREATE TABLE Fact_UserInteraction (
            interaction_id INTEGER PRIMARY KEY,
            date_id INTEGER,
            user_id INTEGER,
            content_id INTEGER,
            ratingValue INTEGER,
            isAddedToList INTEGER,
            FOREIGN KEY(date_id) REFERENCES DimDate(date_id)
            FOREIGN KEY(user_id) REFERENCES DimUser(user_id),
            FOREIGN KEY(content_id) REFERENCES DimContent(content_id)
        )`);
    });

    db.close((err) => {
        if (err) return console.error('Error closing destination DB:', err.message);
        console.log('Destination database schema created and connection closed.');
    });
};

createSourceDatabase();
createDestinationDatabase();