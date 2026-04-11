const admin = require("firebase-admin");
const cron = require("node-cron");

console.log("🚀 Reward Engine Started");

let serviceAccount;

if (process.env.FIREBASE_KEY) {
    serviceAccount = JSON.parse(process.env.FIREBASE_KEY);
} else {
    serviceAccount = require("./serviceAccountKey.json");
}

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://learnixgamereviews-default-rtdb.asia-southeast1.firebasedatabase.app"
});

const db = admin.database();

const DEFAULT_RULES_10000 = [
    { rank: 1, count: 1, amount: 10000 },
    { rank: 2, count: 1, amount: 1000 },
    { rank: 3, count: 3, amount: 500 },
    { rank: 4, count: 4, amount: 100 },
    { rank: 5, count: 5, amount: 50 },
    { rank: 6, count: 986, amount: 5 },
    { rank: 7, count: 9000, amount: 1 }
];

const DEFAULT_RULES_250 = [
    { rank: 1, count: 1, amount: 121 },
    { rank: 2, count: 1, amount: 50 },
    { rank: 3, count: 3, amount: 20 },
    { rank: 4, count: 4, amount: 10 },
    { rank: 5, count: 5, amount: 5 },
    { rank: 6, count: 486, amount: 1 },
    { rank: 7, count: 1000, amount: 0.5 }
];

function getTodayISTTime(hour, minute = 0) {
    const now = new Date();

    const ist = new Date(
        now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
    );

    ist.setHours(hour, minute, 0, 0);

    return ist.getTime();
}

function getNowIST() {
    return new Date(
        new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
    ).getTime();
}

function getWeeklyWindow10k() {
    const now = new Date();

    const ist = new Date(
        now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
    );

    const day = ist.getDay();

    let monday = new Date(ist);
    const diffToMonday = (day === 0 ? -6 : 1 - day);
    monday.setDate(ist.getDate() + diffToMonday);
    monday.setHours(4, 0, 0, 0);

    let sunday = new Date(monday);
    sunday.setDate(monday.getDate() + 6);
    sunday.setHours(23, 30, 0, 0);

   const nowTime = getNowIST();

    if (nowTime > sunday.getTime()) {
        monday.setDate(monday.getDate() + 7);
        sunday.setDate(sunday.getDate() + 7);
    }

    const start = monday.getTime();
    const end = sunday.getTime();

    if (nowTime >= start && nowTime < end) {
        return { active: true, start, end };
    }

    return { active: false, start, end };
}

function getCompetitionWindow250() {
    const now = getNowIST();

    const startMorning = getTodayISTTime(8, 0);
    const endMorning = getTodayISTTime(14, 0);

    const startEvening = getTodayISTTime(15, 0);
    const endEvening = getTodayISTTime(21, 0);

    if (now >= startMorning && now < endMorning) {
        return { active: true, start: startMorning, end: endMorning };
    }

    if (now >= startEvening && now < endEvening) {
        return { active: true, start: startEvening, end: endEvening };
    }

    if (now >= endMorning && now < startEvening) {
        return {
            active: false,
            start: startMorning,
            end: endMorning,
            isResultTime: true
        };
    }

    if (now < startMorning) {
        return { active: false, start: startMorning, end: endMorning };
    }

    return {
        active: false,
        start: getTodayISTTime(8, 0) + 24 * 60 * 60 * 1000,
        end: getTodayISTTime(14, 0) + 24 * 60 * 60 * 1000
    };
}

function parseCustomDate(dateStr) {
    if (!dateStr) return null;

    try {
        const parsed = new Date(dateStr);

        if (!isNaN(parsed)) return parsed.getTime();

        const [datePart, timePart] = dateStr.split("/");
        const [month, day, year] = datePart.split("-").map(Number);
        const [hour, minute, second] = timePart.split(":").map(Number);

        return new Date(year, month - 1, day, hour, minute, second).getTime();

    } catch {
        return null;
    }
}

async function getUsersFromGroup(groupId) {
    try {
        const snap = await db.ref(`RISE-Rewards/${groupId}/usersx`).once("value");

        if (!snap.exists()) return [];

        const data = snap.val();

        const users = Object.keys(data).map(uid => ({
            userId: uid,
            name: data[uid].userName || "User",
            icon: data[uid].userIconIndex || 0,
            points: Number(data[uid].points || 0)
        }));

        return users;
    } catch (err) {
        console.error("❌ Error fetching group:", groupId, err);
        return [];
    }
}

// get all users
async function getAllUsers(groups) {

    let users10000 = [];
    let users250 = [];

    const promises = Object.keys(groups || {}).map(async (groupId) => {
        const users = await getUsersFromGroup(groupId);
        return { groupId, users };
    });

    const results = await Promise.all(promises);

    for (let { groupId, users } of results) {

        if (groupId.toUpperCase().includes("10000RS")) {
            users10000.push(...users);
        }

        if (groupId.toUpperCase().includes("250RS")) {
            users250.push(...users);
        }
    }

    return { users10000, users250 };
}

async function getPrizeRules(type) {
    const snap = await db.ref(`Game-Config/prizeRules/${type}`).once("value");

    if (!snap.exists()) {
        throw new Error("❌ Prize rules not found");
    }

    const data = snap.val();

    return Array.isArray(data) ? data : Object.values(data);
}

// sort users
function sortUsers(users) {
    return users.sort((a, b) => {

        if (b.points !== a.points) return b.points - a.points;

        const nameA = (a.name || "").toLowerCase();
        const nameB = (b.name || "").toLowerCase();

        if (nameA !== nameB) {
            return nameA.localeCompare(nameB);
        }

        return (a.userId || "").localeCompare(b.userId || "");
    });
}

function buildPrizeSlots(prizeRules) {
    const slots = [];

    prizeRules.forEach(rule => {
        for (let i = 0; i < rule.count; i++) {
            slots.push(rule.amount);
        }
    });

    return slots;
}

function getSlotAmount(index, prizeRules) {
    let countSum = 0;

    for (let rule of prizeRules) {
        countSum += rule.count;
        if (index < countSum) return rule.amount;
    }

    return 0;
}

function assign250Prizes(users, prizeRules) {
    if (!users.length) return [];

    const QUALIFY_SCORE = 630;

    const qualified = users
        .filter(u => u.points >= QUALIFY_SCORE)
        .slice(0, 1500); // 1500 limit

    const nonQualified = users.filter(u => u.points < QUALIFY_SCORE);

    const scoreGroups = [];
    let index = 0;

    while (index < qualified.length) {
        const score = qualified[index].points;
        const startIndex = index;
        const groupUsers = [];

        while (index < qualified.length && qualified[index].points === score) {
            groupUsers.push(qualified[index]);
            index++;
        }

        const endIndex = index;
        const rank = startIndex + 1;

        let prizeSum = 0;

        for (let i = startIndex; i < endIndex; i++) {
            prizeSum += getSlotAmount(i, prizeRules);
        }

        scoreGroups.push({
            score,
            rank,
            players: groupUsers,
            prizeSum,
        });
    }

    const blocks = [];

    for (const group of scoreGroups) {
        blocks.push({
            groups: [group],
            totalPlayers: group.players.length,
            totalPrize: group.prizeSum,
            payout() {
                return this.totalPrize / this.totalPlayers;
            },
        });

        while (blocks.length >= 2) {
            const last = blocks[blocks.length - 1];
            const prev = blocks[blocks.length - 2];

            if (last.payout() > prev.payout()) {
                prev.groups.push(...last.groups);
                prev.totalPlayers += last.totalPlayers;
                prev.totalPrize += last.totalPrize;
                blocks.pop();
            } else {
                break;
            }
        }
    }

    const result = [];

    for (const block of blocks) {
        const payout = Math.round(
            (block.totalPrize / block.totalPlayers) * 100
        ) / 100;

        for (const group of block.groups) {
            for (const user of group.players) {
                result.push({
                    ...user,
                    rank: group.rank,
                    prizeAmount: payout,
                });
            }
        }
    }

    for (const user of nonQualified) {
        result.push({
            ...user,
            rank: 999999,
            prizeAmount: 0,
        });
    }

    return result;
}

function assign10000Prizes(users, prizeRules) {
    if (!users.length) return [];

    const QUALIFY_SCORE = 7350;

    const qualified = users
        .filter(u => u.points >= QUALIFY_SCORE)
        .slice(0, 10000);

    const nonQualified = users.filter(u => u.points < QUALIFY_SCORE);

    const scoreGroups = [];
    let index = 0;

    while (index < qualified.length) {
        const score = qualified[index].points;
        const startIndex = index;
        const groupUsers = [];

        while (index < qualified.length && qualified[index].points === score) {
            groupUsers.push(qualified[index]);
            index++;
        }

        const endIndex = index;
        const rank = startIndex + 1;

        let prizeSum = 0;

        for (let i = startIndex; i < endIndex; i++) {
            prizeSum += getSlotAmount(i, prizeRules);
        }

        scoreGroups.push({
            score,
            rank,
            players: groupUsers,
            prizeSum,
        });
    }

    // block 
    const blocks = [];

    for (const group of scoreGroups) {
        blocks.push({
            groups: [group],
            totalPlayers: group.players.length,
            totalPrize: group.prizeSum,
            payout() {
                return this.totalPrize / this.totalPlayers;
            },
        });

        while (blocks.length >= 2) {
            const last = blocks[blocks.length - 1];
            const prev = blocks[blocks.length - 2];

            if (last.payout() > prev.payout()) {
                prev.groups.push(...last.groups);
                prev.totalPlayers += last.totalPlayers;
                prev.totalPrize += last.totalPrize;
                blocks.pop();
            } else {
                break;
            }
        }
    }

    // final result
    const result = [];

    for (const block of blocks) {
        const payout = Math.round(
            (block.totalPrize / block.totalPlayers) * 100
        ) / 100;

        for (const group of block.groups) {
            for (const user of group.players) {
                result.push({
                    ...user,
                    rank: group.rank,
                    prizeAmount: payout,
                });
            }
        }
    }

    // NON QUALIFIED
    for (const user of nonQualified) {
        result.push({
            ...user,
            rank: 999999,
            prizeAmount: 0,
        });
    }

    return result;
}
// prevent duplicate results

async function alreadyProcessed(type, endTime) {
    const snap = await db.ref(`rise-rewards-result/${type}`)
        .orderByChild("endTime")
        .equalTo(endTime)
        .once("value");

    return snap.exists();
}

// save results
async function saveResults(type, users, endTime) {

    const lockRef = db.ref(`locks/${type}/${endTime}`);

const lockSnap = await lockRef.transaction((current) => {
    if (current === true) return;
    return true;
});

if (!lockSnap.committed) {
    console.log(`⏩ Skipped (locked): ${type}`);
    return;
}

    const now = new Date();

    const displayDate = now.toLocaleString("en-IN", {
        weekday: "short",
        day: "2-digit",
        month: "short",
        hour: "2-digit",
        minute: "2-digit"
    });

    const key = endTime;

    const usersObject = {};

    for (let i = 0; i < users.length; i++) {
        const user = users[i];
        if (user.userId) {
            usersObject[user.userId] = user;
        }
    }

    const resultData = {
        createdAt: now.toISOString(),
        displayDate,
        endTime,
        users: usersObject
    };

    // Save result
    await db.ref(`rise-rewards-result/${type}/${key}`).set(resultData);
    await db.ref(`latest-result/${type}`).set(resultData);
    console.log(`📦 Result stored for ${type} | Users: ${Object.keys(usersObject).length}`);
    console.log("📦 Result saved, now updating earnings...");

    // IMPORTANT: process each user safely
   const BATCH_SIZE = 500;

for (let i = 0; i < users.length; i += BATCH_SIZE) {
    console.log(`⚡ Processing batch ${i} → ${i + BATCH_SIZE}`);
    const batch = users.slice(i, i + BATCH_SIZE);

    await Promise.all(batch.map(async (user) => {

        if (!user.userId || user.prizeAmount <= 0) return;

        const userRefPath = `users/${user.userId}`;
        const resultKey = String(endTime);
        const processedPath = `${userRefPath}/processedResults/${type}/${resultKey}`;

        try {
            const lock = await db.ref(processedPath).transaction((current) => {
             if (current === true) return;
            return true;
            });

    if (!lock.committed) return;

    const updates = {};

    if (type === "250rs") {
        updates[`${userRefPath}/totalEarningFromRiseRewards121rs`] =
            admin.database.ServerValue.increment(user.prizeAmount);
    }

    if (type === "10000rs") {
        updates[`${userRefPath}/totalEarningFromRiseRewards10K`] =
            admin.database.ServerValue.increment(user.prizeAmount);
    }

    await db.ref().update(updates);

} catch (err) {
    console.error("❌ Update failed:", user.userId);
}
    }));
}

    console.log("💰 User earnings updated safely (no duplicates)");

    // ✅ Update config
    if (type === "10000rs") {
        await db.ref("Game-Config").update({
            _10K_CompetitionResultID: key
        });
    }

    if (type === "250rs") {
        await db.ref("Game-Config").update({
            _250rs_CompetitionResultID: key
        });
    }

    console.log(`✅ Saved ${type} result with userId keys`);
}

async function shouldProcessResult(type, endTime) {
    const now = getNowIST();

    return now >= endTime;
}

async function processRewards() {
    try {
        const snap = await db.ref("Game-Config").once("value");
        if (!snap.exists()) return;

        const config = snap.val();

        const rewardsSnap = await db.ref("RISE-Rewards").once("value");
        const groups = rewardsSnap.val() || {};

        // 10K RESET + BATCH DELETE USERS
        if (config._10K_CompetitionResultID) {
            const resultTime = Number(config._10K_CompetitionResultID);
            const resetTime = resultTime + (60 * 60 * 1000);

            if (getNowIST() >= resetTime) {

                // Clear result ID
                await db.ref("Game-Config").update({
                    _10K_CompetitionResultID: ""
                });

                // Batch delete usersx
                const updates10k = {};

                for (let groupId in groups) {
                    if (groupId.toUpperCase().includes("10000RS")) {
                        updates10k[`RISE-Rewards/${groupId}/usersx`] = null;
                    }
                }

                if (Object.keys(updates10k).length > 0) {
                    await db.ref().update(updates10k);
                }

                console.log("✅ 10K Result reset + users deleted (batch)");
            }
        }

        // 250 RESET + BATCH DELETE USERS
        if (config._250rs_CompetitionResultID) {
            const resultTime = Number(config._250rs_CompetitionResultID);
            const resetTime = resultTime + (60 * 60 * 1000);

            if (getNowIST() >= resetTime) {

                // Clear result ID
                await db.ref("Game-Config").update({
                    _250rs_CompetitionResultID: ""
                });

                // Batch delete usersx
                const updates250 = {};

                for (let groupId in groups) {
                    if (groupId.toUpperCase().includes("250RS")) {
                        updates250[`RISE-Rewards/${groupId}/usersx`] = null;
                    }
                }

                if (Object.keys(updates250).length > 0) {
                    await db.ref().update(updates250);
                }

                console.log("✅ 250 Result reset + users deleted (batch)");
            }

        }

        const window250 = getCompetitionWindow250();
        const window10k = getWeeklyWindow10k();

      const new10kStart = new Date(window10k.start - (5.5 * 60 * 60 * 1000)).toISOString();
const new10kDuration = (window10k.end - window10k.start) / 1000;

const new250Start = new Date(window250.start - (5.5 * 60 * 60 * 1000)).toISOString();
const new250Duration = (window250.end - window250.start) / 1000;
        // if (
        //     config._10kCompetitionStartingTime !== new10kStart ||
        //     config._10kCompetitionTotalTime !== new10kDuration ||
        //     config._250rsCompetitionStartingTime !== new250Start ||
        //     config._250rsCompetitionTotalTime !== new250Duration ||
        //     config.isCompetitionActive !== window250.active
        // ) {
        //     await db.ref("Game-Config").update({
        //         _10kCompetitionStartingTime: new10kStart,
        //         _10kCompetitionTotalTime: new10kDuration,

        //         _250rsCompetitionStartingTime: new250Start,
        //         _250rsCompetitionTotalTime: new250Duration,

        //         isCompetitionActive: window250.active
        //     });

        //     console.log("🔥 Firebase updated");
        // }

        const isAnyCompetitionActive =
    window250.active || window10k.active;

await db.ref("Game-Config").update({
    _10kCompetitionStartingTime: new10kStart,
    _10kCompetitionTotalTime: new10kDuration,

    _250rsCompetitionStartingTime: new250Start,
    _250rsCompetitionTotalTime: new250Duration,
});

console.log("🔥 Time config FORCE updated");

await db.ref("Game-Config").update({
    isCompetitionActive: isAnyCompetitionActive
});

        // Logs
        console.log("10K Active:", window10k.active);
        console.log("250 Active:", window250.active);

        const start10k = window10k.start;
        const end10k = window10k.end;

        const now = getNowIST();

        const { users250, users10000 } = await getAllUsers(groups);
// ================= 250 SAFE =================

const resultTimes250 = [
    getTodayISTTime(14, 0),
    getTodayISTTime(21, 0)
];

for (const resultEndTime of resultTimes250) {

    if (!(await shouldProcessResult("250rs", resultEndTime))) continue;

    console.log("🚀 Processing 250 result:", new Date(resultEndTime).toLocaleString());

    let prizeRules250;

    try {
        prizeRules250 = await getPrizeRules("250");
    } catch {
        prizeRules250 = DEFAULT_RULES_250;
    }

    prizeRules250.sort((a, b) => a.rank - b.rank);

    const final250 = assign250Prizes(
        sortUsers(users250),
        prizeRules250
    );

    console.log(`👥 Total 250 users: ${users250.length}`);
    console.log(`🏆 Winners 250: ${final250.length}`);
    
    await saveResults("250rs", final250, resultEndTime);

    console.log("✅ 250 RESULT GENERATED");
}

// ================= 10K SAFE =================

if (await shouldProcessResult("10000rs", end10k)) {

    console.log("🚀 Processing 10K result");

    let prizeRules10k;

    try {
        prizeRules10k = await getPrizeRules("10000");
    } catch {
        prizeRules10k = DEFAULT_RULES_10000;
    }

    prizeRules10k.sort((a, b) => a.rank - b.rank);

    const final10k = assign10000Prizes(
        sortUsers(users10000),
        prizeRules10k
    );

    console.log(`👥 Total 10K users: ${users10000.length}`);
    console.log(`🏆 Winners 10K: ${final10k.length}`);
    
    await saveResults("10000rs", final10k, end10k);

    console.log("✅ 10K RESULT GENERATED");
}
} catch (err) {
    console.error("ERROR:", err);
}
    console.log("✅ Reward cycle completed");
}

cron.schedule("* * * * *", processRewards);

processRewards();

const express = require("express");
const app = express();

const PORT = process.env.PORT || 3000;

app.get("/", (req, res) => {
    res.send("Reward Engine Running 🚀");
});

app.listen(PORT, () => {
    console.log(`🌐 Server running on port ${PORT}`);
});
