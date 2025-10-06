const sqlite3 = require('sqlite3').verbose();

const sourceDB = new sqlite3.Database('./source.db');
const destDB = new sqlite3.Database('./destination.db');

const runQuery = (db, sql, params = []) => new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
    });
});

const runStatement = (db, sql, params = []) => new Promise((resolve, reject) => {
    db.run(sql, params, function(err) {
        if (err) reject(err);
        else resolve({ lastID: this.lastID, changes: this.changes });
    });
});

async function etlProcess() {
    try {
        console.log('Starting ETL Process');

        console.log('Extracting data from source.db');
        const users = await runQuery(sourceDB, 'SELECT * FROM users');
        const content = await runQuery(sourceDB, 'SELECT * FROM content');
        const viewings = await runQuery(sourceDB, 'SELECT * FROM viewings');
        const interactions = await runQuery(sourceDB, 'SELECT * FROM interactions');
        const subscriptions = await runQuery(sourceDB, 'SELECT * FROM subscriptions');

        console.log('Transforming data');
        const planMap = new Map();
        subscriptions.forEach(s => {
            if (!planMap.has(s.plan_name)) planMap.set(s.plan_name, {});
        });

        console.log('Loading data into destination.db');

        await new Promise((resolve, reject) => {
            destDB.serialize(async () => {
                try {
                    console.log('Clearing destination tables');
                    await runStatement(destDB, 'DELETE FROM Fact_ViewingActivity');
                    await runStatement(destDB, 'DELETE FROM Fact_SubscriptionTransaction');
                    await runStatement(destDB, 'DELETE FROM Fact_UserInteraction');
                    await runStatement(destDB, 'DELETE FROM DimUser');
                    await runStatement(destDB, 'DELETE FROM DimContent');
                    await runStatement(destDB, 'DELETE FROM DimDate');
                    await runStatement(destDB, 'DELETE FROM DimSubscriptionPlan');

                    console.log('Loading DimUser');
                    for (const user of users) await runStatement(destDB, 'INSERT INTO DimUser (user_id, fullName, email, joinDate) VALUES (?, ?, ?, ?)', [user.id, user.full_name, user.email, user.join_date]);

                    console.log('Loading DimContent');
                    for (const item of content) await runStatement(destDB, 'INSERT INTO DimContent (content_id, title, type, mainGenre, releaseYear) VALUES (?, ?, ?, ?, ?)', [item.id, item.title, item.type, item.genre, item.release_year]);

                    console.log('Loading DimDate');
                    const allDates = [...viewings.map(v => v.view_date), ...interactions.map(i => i.interaction_date), ...subscriptions.map(s => s.event_date)];
                    const uniqueDates = [...new Set(allDates.map(d => d.split(' ')[0]))];
                    for (const d_str of uniqueDates) {
                        const d = new Date(d_str);
                        const dateId = parseInt(`${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`);
                        await runStatement(destDB, 'INSERT INTO DimDate (date_id, fullDate, year, quarter, month, dayOfWeek) VALUES (?, ?, ?, ?, ?, ?)', [dateId, d_str, d.getFullYear(), Math.floor(d.getMonth() / 3) + 1, d.getMonth() + 1, d.getDay()]);
                    }

                    console.log('Loading DimSubscriptionPlan');
                    const dimSubscriptionPlan = Array.from(planMap.keys());
                    for (const plan of dimSubscriptionPlan) await runStatement(destDB, 'INSERT INTO DimSubscriptionPlan (planName) VALUES (?)', [plan]);
                    
                    const plans = await runQuery(destDB, 'SELECT * FROM DimSubscriptionPlan');
                    plans.forEach(p => planMap.set(p.planName, { id: p.plan_id }));

                    console.log('Loading Fact_ViewingActivity');
                    for (const v of viewings) {
                        const d = new Date(v.view_date);
                        const dateId = parseInt(`${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`);
                        await runStatement(destDB, 'INSERT INTO Fact_ViewingActivity (date_id, user_id, content_id, durationWatchedMinutes) VALUES (?, ?, ?, ?)', [dateId, v.user_id, v.content_id, v.duration_watched_minutes]);
                    }

                    console.log('Loading Fact_SubscriptionTransaction');
                    for (const s of subscriptions) {
                        const d = new Date(s.event_date);
                        const dateId = parseInt(`${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`);
                        const planId = planMap.get(s.plan_name)?.id;
                        if (planId) await runStatement(destDB, 'INSERT INTO Fact_SubscriptionTransaction (date_id, user_id, plan_id, transactionAmount) VALUES (?, ?, ?, ?)', [dateId, s.user_id, planId, s.amount]);
                    }

                    console.log('Loading Fact_UserInteraction');
                    for (const i of interactions) {
                        const d = new Date(i.interaction_date);
                        const dateId = parseInt(`${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`);
                        const isAddedToList = i.interaction_type === 'ADD_TO_LIST' ? 1 : 0;
                        const ratingValue = i.interaction_type === 'RATING' ? i.rating_value : 0;
                        await runStatement(destDB, 'INSERT INTO Fact_UserInteraction (date_id, user_id, content_id, ratingValue, isAddedToList) VALUES (?, ?, ?, ?, ?)', [dateId, i.user_id, i.content_id, ratingValue, isAddedToList]);
                    }
                    
                    resolve();
                } catch (err) {
                    reject(err);
                }
            });
        });

        console.log('ETL process completed successfully');

    } catch (err) {
        console.error('ETL process failed:', err);
    } finally {
        sourceDB.close();
        destDB.close();
        console.log('All database connections closed.');
    }
}

etlProcess();