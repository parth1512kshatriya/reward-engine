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

let timerSet250 = false;
let timerSet10k = false;

function getTodayISTTime(hour, minute = 0) {
    const now = new Date();

    const ist = new Date(
        now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
    );

    ist.setHours(hour, minute, 0, 0);

    return ist.getTime();
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

    const nowTime = ist.getTime();

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
    const now = Date.now();

    const startMorning = getTodayISTTime(8);
    const endMorning = getTodayISTTime(14);

    const startEvening = getTodayISTTime(15);
    const endEvening = getTodayISTTime(21);

    if (now >= startMorning && now < endMorning) {
        return { active: true, start: startMorning, end: endMorning };
    }

    if (now >= startEvening && now < endEvening) {
        return { active: true, start: startEvening, end: endEvening };
    }

    if (now < startMorning) {
        return { active: false, start: startMorning, end: endMorning };
    }

    if (now >= endMorning && now < startEvening) {
        return { active: false, start: startMorning, end: endMorning };
    }

    return { active: false, start: startEvening, end: endEvening };
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

// function assignRanksAndPrizes(users, prizeRules) {
//     if (!users.length) return [];

//     const result = [];

//     let i = 0;
//     let displayRank = 1;

//     const is10k = prizeRules[0].amount === 10000;
//     const minPoints = is10k ? 7350 : 630; // ✅ updated from Kotlin logic

//     // ✅ CREATE SLOT SYSTEM FOR ₹250
//     let prizeSlots = [];

//     if (!is10k) {
//         prizeSlots = [];

//         prizeRules.forEach(rule => {
//             for (let k = 0; k < rule.count; k++) {
//                 prizeSlots.push(rule.amount);
//             }
//         });
//     }

//     let currentSlotIndex = 0;

//     while (i < users.length) {

//         // AFTER RANK 6 → SAME LOGIC
//         if (displayRank > 6) {

//             while (i < users.length) {

//                 const user = users[i];

//                 let prize = 0;

//                 if (user.points >= minPoints) {
//                     prize = is10k ? 1 : 0.5;
//                 }

//                 result.push({
//                     ...user,
//                     rank: 7,
//                     prizeAmount: prize
//                 });

//                 i++;
//             }

//             break;
//         }

//         // CREATE TIE GROUP
//         let j = i;
//         while (j < users.length && users[j].points === users[i].points) j++;

//         const tieGroup = users.slice(i, j);
//         const tieCount = tieGroup.length;

//         let perUserPrize = 0;

//         if (is10k) {
//             // 🔥 KEEP YOUR OLD 10K LOGIC
//             const currentRule = prizeRules.find(r => r.rank === displayRank);

//             if (!currentRule) {
//                 tieGroup.forEach(user => {
//                     result.push({
//                         ...user,
//                         rank: displayRank,
//                         prizeAmount: 0
//                     });
//                 });

//                 displayRank++;
//                 i = j;
//                 continue;
//             }

//             if (tieCount === 1) {
//                 perUserPrize = currentRule.amount;
//             } else {
//                 let totalPool = 0;

//                 for (let r = displayRank; r <= 7; r++) {
//                     const rule = prizeRules.find(x => x.rank === r);
//                     if (rule) totalPool += rule.amount;
//                 }

//                 perUserPrize = totalPool / tieCount;
//             }

//         } else {
//             // ✅ ₹250 SLOT-BASED LOGIC (FROM YOUR KOTLIN)

//             let totalPrize = 0;

//             if (currentSlotIndex >= prizeSlots.length) {

//                 const TOTAL_POOL = prizeSlots.reduce((sum, val) => sum + val, 0);
//                 perUserPrize = TOTAL_POOL / tieCount;

//             } else if (currentSlotIndex + tieCount > prizeSlots.length) {

//                 const TOTAL_POOL = prizeSlots.reduce((sum, val) => sum + val, 0);
//                 perUserPrize = TOTAL_POOL / tieCount;

//             } else {

//                 for (let k = 0; k < tieCount; k++) {
//                     totalPrize += prizeSlots[currentSlotIndex + k];
//                 }

//                 perUserPrize = totalPrize / tieCount;
//             }
//         }

//         // ASSIGN
//         for (let user of tieGroup) {

//             const isQualified = user.points >= minPoints;

//             let prize = 0;

//             if (isQualified) {
//                 if (displayRank === 7) {
//                     prize = is10k ? 1 : 0.5;
//                 } else {
//                     prize = parseFloat(perUserPrize.toFixed(4)); // Kotlin precision
//                 }
//             }

//             result.push({
//                 ...user,
//                 rank: displayRank,
//                 prizeAmount: prize
//             });
//         }

//         if (!is10k) {
//             currentSlotIndex += tieCount; // ✅ IMPORTANT
//         }

//         displayRank++;
//         i = j;
//     }

//     return result;
// }

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

    if (await alreadyProcessed(type, endTime)) {
        console.log(`⏩ Already processed ${type}`);
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

    console.log("📦 Result saved, now updating earnings...");

    // IMPORTANT: process each user safely
    for (const user of users) {

        if (!user.userId || user.prizeAmount <= 0) continue;

        const userRefPath = `users/${user.userId}`;

        const userSnap = await db.ref(userRefPath).once("value");

        if (!userSnap.exists()) {
            console.log("⏩ Skipping missing user:", user.userId);
            continue;
        }

        const resultKey = String(endTime);

        const processedPath = `${userRefPath}/processedResults/${type}/${resultKey}`;

        // ATOMIC LOCK (THIS IS THE MAIN FIX)
        await db.ref(processedPath).transaction(async (current) => {

            if (current === true) {
                return;
            }

            return true;

        }, async (error, committed) => {

            if (error) {
                console.error("Transaction error:", error);
                return;
            }

            if (!committed) {
                return;
            }

            try {
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
                console.error("❌ Earnings update failed:", err);
            }
        });
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

            if (Date.now() >= resetTime) {

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

            if (Date.now() >= resetTime) {

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

        const new10kStart = new Date(window10k.start).toISOString();
        const new10kDuration = (window10k.end - window10k.start) / 1000;

        const new250Start = new Date(window250.start).toISOString();
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

        const isAnyCompetitionActive = window250.active || window10k.active;

        if (
            config._10kCompetitionStartingTime !== new10kStart ||
            config._10kCompetitionTotalTime !== new10kDuration ||
            config._250rsCompetitionStartingTime !== new250Start ||
            config._250rsCompetitionTotalTime !== new250Duration
        ) {
            await db.ref("Game-Config").update({
                _10kCompetitionStartingTime: new10kStart,
                _10kCompetitionTotalTime: new10kDuration,

                _250rsCompetitionStartingTime: new250Start,
                _250rsCompetitionTotalTime: new250Duration,
            });

            console.log("🔥 Time config updated");
        }

        await db.ref("Game-Config").update({
            isCompetitionActive: isAnyCompetitionActive
        });

        // Logs
        console.log("10K Active:", window10k.active);
        console.log("250 Active:", window250.active);

        const start10k = window10k.start;
        const end10k = window10k.end;

        const start250 = window250.start;
        const end250 = window250.end;

        const now = Date.now();

        const delay250 = end250 - now;
        const delay10k = end10k - now;

        //  HANDLE MISSED INSTANT EXECUTION (CRITICAL FIX)

        if (delay10k <= 0 && delay10k > -60000 && !timerSet10k) {
            console.log("⚡ Missed 10K exact timing → running instantly");

            timerSet10k = true;

            (async () => {
                const { users10000 } = await getAllUsers(groups);

                if (users10000.length) {
                    let prizeRules10k;

                    try {
                        prizeRules10k = await getPrizeRules("10000");
                    } catch {
                        prizeRules10k = DEFAULT_RULES_10000;
                    }

                    prizeRules10k.sort((a, b) => a.rank - b.rank);

                    const final10k = assign10000Prizes(sortUsers(users10000), prizeRules10k);

                    if (!(await alreadyProcessed("10000rs", end10k))) {
                        await saveResults("10000rs", final10k, end10k);
                    }
                }

                timerSet10k = false;
            })();
        }

        if (delay250 <= 0 && delay250 > -60000 && !timerSet250) {
            console.log("⚡ Missed 250 timing → running instantly");

            timerSet250 = true;

            (async () => {
                const { users250 } = await getAllUsers(groups);

                if (users250.length) {
                    let prizeRules250;

                    try {
                        prizeRules250 = await getPrizeRules("250");
                    } catch {
                        prizeRules250 = DEFAULT_RULES_250;
                    }

                    prizeRules250.sort((a, b) => a.rank - b.rank);

                    const final250 = assign250Prizes(sortUsers(users250), prizeRules250);

                    if (!(await alreadyProcessed("250rs", end250))) {
                        await saveResults("250rs", final250, end250);
                    }
                }

                timerSet250 = false;
            })();
        }

        if (!timerSet250 && delay250 > 0 && delay250 < 60000) {
            timerSet250 = true;

            setTimeout(async () => {
                console.log("⚡ Instant 250 result");

                timerSet250 = false;

                const { users250 } = await getAllUsers(groups);

                if (users250.length) {
                    let prizeRules250;

                    try {
                        prizeRules250 = await getPrizeRules("250");
                    } catch {
                        prizeRules250 = DEFAULT_RULES_250;
                    }

                    prizeRules250.sort((a, b) => a.rank - b.rank);

                    const final250 = assign250Prizes(sortUsers(users250), prizeRules250);

                    if (!(await alreadyProcessed("250rs", end250))) {
                        await saveResults("250rs", final250, end250);
                    }
                }
            }, delay250);
        }

        if (!timerSet10k && delay10k > 0 && delay10k < 60000) {
            timerSet10k = true;

            setTimeout(async () => {
                console.log("⚡ Instant 10K result");

                timerSet10k = false;

                const { users10000 } = await getAllUsers(groups);

                if (users10000.length) {
                    let prizeRules10k;

                    try {
                        prizeRules10k = await getPrizeRules("10000");
                    } catch {
                        prizeRules10k = DEFAULT_RULES_10000;
                    }

                    prizeRules10k.sort((a, b) => a.rank - b.rank);

                    const final10k = assign10000Prizes(sortUsers(users10000), prizeRules10k);

                    if (!(await alreadyProcessed("10000rs", end10k))) {
                        await saveResults("10000rs", final10k, end10k);
                    }
                }
            }, delay10k);
        }

        const is10000Done =
            now >= end10k &&
            now <= end10k + (5 * 60 * 1000); // 5 min window for weekly safety
        const is250Done = now >= end250 && now <= end250 + 60000;

        console.log("NOW:", new Date(now).toLocaleString());
        console.log("END 10K:", new Date(end10k).toLocaleString());
        console.log("END 250:", new Date(end250).toLocaleString());
        console.log("is10000Done:", is10000Done);
        console.log("is250Done:", is250Done);

        console.log("10K END:", new Date(end10k).toLocaleString(), is10000Done);

        console.log("250 END:", new Date(end250).toLocaleString(), is250Done);

        if (!is10000Done && !is250Done) {
            return;
        }
        if (
            (is10000Done && await alreadyProcessed("10000rs", end10k)) &&
            (is250Done && await alreadyProcessed("250rs", end250))
        ) {
            return;
        }

        let users10000 = [];
        let users250 = [];

        if (is10000Done) {
            users10000 = (await getAllUsers(groups)).users10000;
        }

        if (is250Done) {
            users250 = (await getAllUsers(groups)).users250;
        }
        console.log("Users 10K:", users10000.length);
        console.log("Users 250:", users250.length);

        if (is10000Done) {

            const sorted10k = sortUsers(users10000);

            let prizeRules10k;

            try {
                prizeRules10k = await getPrizeRules("10000");
            } catch (e) {
                prizeRules10k = DEFAULT_RULES_10000;
            }

            prizeRules10k = prizeRules10k.sort((a, b) => a.rank - b.rank);

            const final10k = assign10000Prizes(sorted10k, prizeRules10k);

            if (!(await alreadyProcessed("10000rs", end10k))) {
                await saveResults("10000rs", final10k, end10k);
            }

            console.log("🔁 10K restarted for next weekly cycle");
        }

        if (is250Done) {

            const sorted250 = sortUsers(users250);

            if (sorted250.length) {

                let prizeRules250;

                try {
                    prizeRules250 = await getPrizeRules("250");
                } catch (e) {
                    prizeRules250 = DEFAULT_RULES_250;
                }

                prizeRules250 = prizeRules250.sort((a, b) => a.rank - b.rank);

                const final250 = assign250Prizes(sorted250, prizeRules250);

                if (!(await alreadyProcessed("250rs", end250))) {
                    await saveResults("250rs", final250, end250);
                }
            }

            console.log("🔁 250 restarted for next 6hr cycle");
        }

    } catch (err) {
        console.error("ERROR:", err);
    }
}

// run every minute
cron.schedule("* * * * *", processRewards);

// run immediately
processRewards();