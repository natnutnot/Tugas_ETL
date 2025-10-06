const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database('./source.db', (err) => {
    if (err) return console.error(err.message);
    console.log('Connected to source.db for seeding.');
});

const users = [
    { id: 101, name: 'Grace', email: 'grace@mail.com', join_date: '2024-01-10' },
    { id: 102, name: 'Erika', email: 'erika@mail.com', join_date: '2024-02-15' },
    { id: 103, name: 'Nopi', email: 'nopi@mail.com', join_date: '2024-03-20' },
    { id: 104, name: 'Ratna', email: 'ratna@mail.com', join_date: '2024-04-25' },
    { id: 105, name: 'Abuk', email: 'abuk@mail.com', join_date: '2024-05-01' },
    { id: 106, name: 'Amay', email: 'amay@mail.com', join_date: '2024-06-05' },
    { id: 107, name: 'Iky', email: 'iky@mail.com', join_date: '2024-07-10' },
    { id: 108, name: 'Iki', email: 'iki@mail.com', join_date: '2024-08-15' },
    { id: 109, name: 'Randy', email: 'randy@mail.com', join_date: '2024-09-20' },
    { id: 110, name: 'Affai', email: 'affai@mail.com', join_date: '2024-10-01' },
];

const content = [
    { id: 201, title: 'Inception', genre: 'Sci-Fi', year: 2010, type: 'Movie' },
    { id: 202, title: 'The Dark Knight', genre: 'Action', year: 2008, type: 'Movie' },
    { id: 203, title: 'Parasite', genre: 'Thriller', year: 2019, type: 'Movie' },
    { id: 204, title: 'Avengers: Endgame', genre: 'Action', year: 2019, type: 'Movie' },
    { id: 205, title: 'Spirited Away', genre: 'Animation', year: 2001, type: 'Movie' },
    { id: 206, title: 'Stranger Things', genre: 'Sci-Fi', year: 2016, type: 'Series' },
];

const viewings = [
    { user_id: 101, content_id: 201, date: '2025-09-01 20:00:00', duration: 148, device: 'Smart TV' },
    { user_id: 102, content_id: 206, date: '2025-09-01 21:00:00', duration: 51, device: 'Mobile' },
    { user_id: 101, content_id: 202, date: '2025-09-02 20:00:00', duration: 152, device: 'Smart TV' },
    { user_id: 105, content_id: 203, date: '2025-09-03 22:00:00', duration: 132, device: 'Web' },
    { user_id: 109, content_id: 204, date: '2025-09-04 19:00:00', duration: 181, device: 'Smart TV' },
    { user_id: 101, content_id: 206, date: '2025-09-04 21:00:00', duration: 49, device: 'Tablet' },
];

const interactions = [
    { user_id: 101, content_id: 201, date: '2025-09-01 22:30:00', type: 'RATING', rating: 1 },
    { user_id: 102, content_id: 206, date: '2025-09-01 22:00:00', type: 'ADD_TO_LIST', rating: null },
    { user_id: 105, content_id: 203, date: '2025-09-04 00:15:00', type: 'RATING', rating: 1 },
];

const subscriptions = [
    { user_id: 101, date: '2025-09-01', plan: 'Premium', type: 'RENEWAL', amount: 186000 },
    { user_id: 102, date: '2025-09-01', plan: 'Standard', type: 'RENEWAL', amount: 120000 },
    { user_id: 110, date: '2025-09-01', plan: 'Basic', type: 'NEW', amount: 65000 },
];

db.serialize(() => {
    const insertUser = db.prepare('INSERT INTO users (id, full_name, email, join_date) VALUES (?, ?, ?, ?)');
    users.forEach(u => insertUser.run(u.id, u.name, u.email, u.join_date));
    insertUser.finalize();

    const insertContent = db.prepare('INSERT INTO content (id, title, genre, release_year, type) VALUES (?, ?, ?, ?, ?)');
    content.forEach(c => insertContent.run(c.id, c.title, c.genre, c.year, c.type));
    insertContent.finalize();
    
    const insertViewing = db.prepare('INSERT INTO viewings (user_id, content_id, view_date, duration_watched_minutes, device) VALUES (?, ?, ?, ?, ?)');
    viewings.forEach(v => insertViewing.run(v.user_id, v.content_id, v.date, v.duration, v.device));
    insertViewing.finalize();

    const insertInteraction = db.prepare('INSERT INTO interactions (user_id, content_id, interaction_date, interaction_type, rating_value) VALUES (?, ?, ?, ?, ?)');
    interactions.forEach(i => insertInteraction.run(i.user_id, i.content_id, i.date, i.type, i.rating));
    insertInteraction.finalize();

    const insertSubscription = db.prepare('INSERT INTO subscriptions (user_id, event_date, plan_name, event_type, amount) VALUES (?, ?, ?, ?, ?)');
    subscriptions.forEach(s => insertSubscription.run(s.user_id, s.date, s.plan, s.type, s.amount));
    insertSubscription.finalize();

    console.log('Sample data has been inserted into source.db.');
});

db.close((err) => {
    if (err) return console.error(err.message);
    console.log('Seeding complete. Connection closed.');
});