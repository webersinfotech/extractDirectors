const mysql      = require('mysql');
const util = require('util');
const axios = require('axios');
const { performance } = require('perf_hooks');
const { cpus } = require('os');
const cluster = require('cluster');
const child_process = require('child_process');
const { uuid } = require('uuidv4');
const fs = require('fs-extra');

// (async () => {
//     const connection = mysql.createConnection({
//         host:'google-account.cmlfk75xsv3h.ap-south-1.rds.amazonaws.com', 
//         user: 'shahrushabh1996', 
//         database: 'rapidTax',
//         password: '11999966',
//         ssl: 'Amazon RDS'
//     }) 
    
//     const query = util.promisify(connection.query).bind(connection)

//     const connections = new Array(65).fill(0)

//     for (let [index, connection] of connections.entries()) {
//         try {
//             const directors = await query(`SELECT * FROM directors WHERE extractionStatus = 'PROCESSING' OR extractionStatus = 'FAILED' ORDER BY id ASC LIMIT 2000`)
    
//             console.log('Directors Fetched')
    
//             const directorIds = []
    
//             directors.map((director) => directorIds.push(director.id))
    
//             console.log('Director ID array ready')
    
//             if (directorIds.length) {
//                 await query(`UPDATE directors SET extractionStatus = ? WHERE id IN (?)`, ['NOT STARTED', directorIds])
//             }
    
//             console.log('Updating extractionStatus')
//         } catch (e) {
//             console.error(e)
//         }
//     }

//     connection.destroy()

//     process.exit()
// })()

(async () => {
    function timeout(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    if (cluster.isMaster) {
        console.log(`Primary ${process.pid} is running`);

        if (!fs.pathExistsSync('./totalProcesses.txt')) {
            fs.writeFileSync('./totalProcesses.txt', '0', 'utf8')
        }

        const connections = new Array(10).fill(0)

        const processses = []

        for (let [index, connection] of connections.entries()) {
            await launchCluster()
        }

        async function launchCluster() {
            const clusterLaunch = cluster.fork()
            clusterLaunch.on('exit', async (worker, code, signal) => {
                console.log(`worker died`);
    
                await launchCluster()
            });
            await timeout(10000)
            return
        }
    } else {
        (async () => {
            const connection = mysql.createConnection({
                host:'google-account.cmlfk75xsv3h.ap-south-1.rds.amazonaws.com', 
                user: 'shahrushabh1996', 
                database: 'rapidTax',
                password: '11999966',
                ssl: 'Amazon RDS'
            })         
        
            const query = util.promisify(connection.query).bind(connection)
        
            let directors
        
            try {
                console.log('Transaction begin')
        
                await connection.beginTransaction()
        
                directors = await query(`SELECT * FROM directors WHERE extractionStatus = 'NOT STARTED' ORDER BY id ASC LIMIT 2000`)
        
                console.log('Directors Fetched')
        
                const directorIds = []
        
                directors.map((director) => directorIds.push(director.id))
        
                console.log('Director ID array ready')
        
                if (directorIds.length) {
                    await query(`UPDATE directors SET extractionStatus = ? WHERE id IN (?)`, ['PROCESSING', directorIds])
                }
        
                console.log('Updating extractionStatus')
        
                await connection.commit()
            } catch (err) {
                console.log(err)
                connection.rollback()
            }
        
            for (let director of directors) {
                const t0 = performance.now()
        
                console.log(`${director.DIN} ::: Process Started`)
        
                try {
                    const companies = await getResponse(director.DIN)

                    if (companies.length) await query('INSERT INTO directorData (id, CIN, dateJoin, dateResign, designation, companyName, companyStatus, paidupCapital) VALUES ?',
                    [companies.map(company => [uuid(), company.CIN, company.DATE_JOIN, company.DATE_RESIGN, company.DESIGNATION, company.COMPANY_NAME, company.COMPANY_STATUS, company.PAIDUP_CAPITAL])])
        
                    await query(`UPDATE directors SET extractionStatus = ? WHERE id IN (?)`, ['SUCCESS', director.id])
        
                    const t1 = performance.now()
                            
                    console.log(`${director.DIN} ::: SUCCESS Time took ${((t1 - t0) / 1000)} seconds.`)
                    
                } catch (err) {
                    const t1 = performance.now()
        
                    console.log(`${director.DIN} ::: FAILED Time took ${((t1 - t0) / 1000)} seconds.`)
        
                    console.log(err)
        
                    await query(`UPDATE directors SET extractionStatus = ? WHERE id IN (?)`, ['FAILED', director.id])
                }
            }

            connection.destroy()

            process.exit()
        })()

        async function getResponse(DIN) {
            return new Promise((res, rej) => {
                var config = {
                    method: 'get',
                    url: `https://api.finanvo.in/director/companies?DIN=${DIN}`,
                    headers: { 
                        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"', 
                        'app-origin': 'https://finanvo.in', 
                        'sec-ch-ua-mobile': '?0', 
                        'Authorization': '', 
                        'Content-Type': 'application/json', 
                        'Accept': 'application/json, text/plain, */*', 
                        'Referer': 'https://finanvo.in/', 
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
                    }
                };
                
                axios(config)
                .then((response) => {
                    res(response.data.data);
                })
                .catch((error) => {
                    rej(error);
                });
            })
        }
    }
})()