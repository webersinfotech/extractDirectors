const mysql      = require('mysql');
const util = require('util');
const { performance } = require('perf_hooks');
const cluster = require('cluster');
const puppeteer = require('puppeteer');
const { uuid } = require('uuidv4');

const blockedUrls = [
    'https://www.google.com/recaptcha/api.js?hl=en',
    'https://netdna.bootstrapcdn.com/bootstrap/3.0.2/css/bootstrap.min.css',
    'https://can.zaubacorp.com/sites/default/files/advagg_css/css__5xaPABKnR4Wolg40v2uxPQ4Se5LA6DGfUvls9iidSRQ__8_rEbSJYTg3yUEaujYLIoIH6HKtuNbwTTd9nCPnIogc__BVGW6IqBymasWhua-L7Fk4Uj4kaS9QvjoGK69oIFF58.css',
    'https://netdna.bootstrapcdn.com/bootstrap/3.0.2/js/bootstrap.min.js',
    'https://js.stripe.com/v3',
    'https://can.zaubacorp.com/sites/default/files/advagg_js/js__-yHP6av1S52VzkYW8bfgSJLkkH7PLKCbfffpfRjcKL0__y3Brw439-Wmboy0KAPaXUnX4eS75QbCJa1aYATnDr38__BVGW6IqBymasWhua-L7Fk4Uj4kaS9QvjoGK69oIFF58.js',
    'https://can.zaubacorp.com/sites/default/files/advagg_js/js__tpReSZ52zE2ghVPPQAm-ESKnVSmwIRHKpG4QJStv7vM__DubtCiNE6jrpmpi2z6_OWr74AGn-afZtMXQUeBXue-s__BVGW6IqBymasWhua-L7Fk4Uj4kaS9QvjoGK69oIFF58.js',
    'https://can.zaubacorp.com/sites/default/files/advagg_js/js___IBOfq3sr4R25I8QLjoPefEhKxKaXMwQlGzLAZkeAmk__Tgy2Gm7LmUJY8GXZeWxVbS51f3txED35LX1ul4UiOfk__BVGW6IqBymasWhua-L7Fk4Uj4kaS9QvjoGK69oIFF58.js',
    'https://www.zaubacorp.com/sites/default/themes/bootstrap_subtheme/images/logo-Zauba.png',
    'https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js',
    'https://www.zaubacorp.com/sites/default/themes/bootstrap_subtheme/images/new/fb2.png',
    'https://www.zaubacorp.com/sites/default/themes/bootstrap_subtheme/images/new/twitter2.png',
    'https://www.gstatic.com/recaptcha/releases/Q_rrUPkK1sXoHi4wbuDTgcQR/recaptcha__en.js',
    'https://www.zaubacorp.com/sites/default/themes/bootstrap_subtheme/images/new/linkedin2.png',
    'https://www.zaubacorp.com/sites/default/themes/bootstrap_subtheme/images/footLogo.png',
    'https://cdn.rawgit.com/bramstein/fontfaceobserver/master/fontfaceobserver.js',
    'https://can.zaubacorp.com/sites/default/files/advagg_js/js__HxTbhN-HTG8H6pU8ZNTXLhmTeg8fhzG_sbeVLz0gBqU__K4ApKepJPcurZyZkfZbfF4bND8mRif4uimvll4yTanU__BVGW6IqBymasWhua-L7Fk4Uj4kaS9QvjoGK69oIFF58.js',
    'https://www.google-analytics.com/analytics.js',
    'https://can.zaubacorp.com/sites/all/modules/responsive_share_buttons/images/buttons/facebook.jpg',
    'https://can.zaubacorp.com/sites/all/modules/responsive_share_buttons/images/buttons/twitter.jpg',
    'https://can.zaubacorp.com/sites/all/modules/responsive_share_buttons/images/buttons/digg.jpg',
    'https://can.zaubacorp.com/sites/all/modules/responsive_share_buttons/images/buttons/stumbleupon.jpg',
    'https://can.zaubacorp.com/sites/all/modules/responsive_share_buttons/images/buttons/delicious.jpg',
    'https://can.zaubacorp.com/sites/all/modules/responsive_share_buttons/images/buttons/google.jpg',
    'https://can.zaubacorp.com/sites/default/themes/bootstrap_subtheme/images/search_bg.png',
    'https://can.zaubacorp.com/sites/default/themes/bootstrap_subtheme/images/searchbtn.png',
    'https://can.zaubacorp.com/sites/default/themes/bootstrap_subtheme/fonts/glyphicons-halflings-regular.woff',
    'https://can.zaubacorp.com/sites/default/themes/bootstrap_subtheme/fonts/ubuntu-r-webfont.woff',
    'https://can.zaubacorp.com/sites/default/themes/bootstrap_subtheme/fonts/opensans-semibold-webfont.woff',
    'https://can.zaubacorp.com/sites/default/themes/bootstrap_subtheme/fonts/glyphicons-halflings-regular.woff',
    'https://can.zaubacorp.com/sites/default/themes/bootstrap_subtheme/fonts/ubuntu-r-webfont.woff',
    'https://can.zaubacorp.com/sites/default/themes/bootstrap_subtheme/images/footeraerrow.png',
    'https://can.zaubacorp.com/sites/default/themes/bootstrap_subtheme/fonts/opensans-semibold-webfont.woff',
    'https://js.stripe.com/v3/m-outer-5564a2ae650989ada0dc7f7250ae34e9.html',
    'https://www.google-analytics.com/plugins/ua/linkid.js',
    'https://www.google.com/recaptcha/api2/anchor?ar=1&k=6LdYZ_8SAAAAABtRvAuoXxXPS7xOFQ8rVTDbRGNV&co=aHR0cHM6Ly93d3cuemF1YmFjb3JwLmNvbTo0NDM.&hl=en&type=image&v=Q_rrUPkK1sXoHi4wbuDTgcQR&theme=light&size=normal&cb=vt1hiqcnon4w',
    'https://stats.g.doubleclick.net/j/collect?t=dc&aip=1&_r=3&v=1&_v=j93&tid=UA-56482455-1&cid=1664612034.1630248415&jid=81082429&gjid=1419951062&_gid=925698442.1630248415&_u=aGBAgEAjAAAAAE~&z=1062428126',
    'https://www.google-analytics.com/collect?v=1&_v=j93&aip=1&a=1791176061&t=pageview&_s=1&dl=https%3A%2F%2Fwww.zaubacorp.com%2Fcompanybrowse&dp=%2Fcompanybrowse&ul=en-gb&de=UTF-8&dt=Companies%20List%20-%20Names%20Starting%20with%20A%20-%20Page%201%20%7C%20Zauba%20Corp&sd=30-bit&sr=1792x1120&vp=1200x850&je=0&_u=aGBAgEAj~&jid=81082429&gjid=1419951062&cid=1664612034.1630248415&tid=UA-56482455-1&_gid=925698442.1630248415&z=821253716',
    'https://pagead2.googlesyndication.com/pagead/managed/js/adsense/m202108240101/show_ads_impl_fy2019.js',
    'https://googleads.g.doubleclick.net/pagead/html/r20210824/r20190131/zrt_lookup.html',
    'https://www.google.com/ads/ga-audiences?t=sr&aip=1&_r=4&slf_rd=1&v=1&_v=j93&tid=UA-56482455-1&cid=1664612034.1630248415&jid=81082429&_u=aGBAgEAjAAAAAE~&z=1368337780',
    'https://www.google.co.in/ads/ga-audiences?t=sr&aip=1&_r=4&slf_rd=1&v=1&_v=j93&tid=UA-56482455-1&cid=1664612034.1630248415&jid=81082429&_u=aGBAgEAjAAAAAE~&z=1368337780',
    'https://www.google.com/recaptcha/api2/bframe?hl=en&v=Q_rrUPkK1sXoHi4wbuDTgcQR&k=6LdYZ_8SAAAAABtRvAuoXxXPS7xOFQ8rVTDbRGNV&cb=e5fj6x92dvni',
    'https://partner.googleadservices.com/gampad/cookie.js?domain=www.zaubacorp.com&callback=_gfp_s_&client=ca-pub-3139678088996753',
    'https://adservice.google.co.in/adsid/integrator.js?domain=www.zaubacorp.com',
    'https://adservice.google.com/adsid/integrator.js?domain=www.zaubacorp.com',
    'https://googleads.g.doubleclick.net/pagead/ads?client=ca-pub-3139678088996753&output=html&h=90&slotname=6652419027&adk=4231419329&adf=395949206&pi=t.ma~as.6652419027&w=728&lmt=1630248415&psa=0&format=728x90&url=https%3A%2F%2Fwww.zaubacorp.com%2Fcompanybrowse&flash=0&wgl=1&uach=WyJtYWNPUyIsIjEwLjE1LjciLCJ4ODYiLCIiLCI5My4wLjQ1NzcuMCIsW10sbnVsbCxudWxsLCI2NCJd&tt_state=W3siaXNzdWVyT3JpZ2luIjoiaHR0cHM6Ly9hdHRlc3RhdGlvbi5hbmRyb2lkLmNvbSIsInN0YXRlIjo3fV0.&dt=1630248414928&bpp=15&bdt=910&idt=433&shv=r20210824&mjsv=m202108240101&ptt=9&saldr=aa&abxe=1&correlator=6198596691614&frm=20&pv=2&ga_vid=1664612034.1630248415&ga_sid=1630248415&ga_hid=1791176061&ga_fc=0&ga_wpids=UA-56482455-1&u_tz=330&u_his=2&u_java=0&u_h=1120&u_w=1792&u_ah=1017&u_aw=1792&u_cd=30&u_nplug=3&u_nmime=4&adx=251&ady=400&biw=1200&bih=850&scr_x=0&scr_y=0&eid=21067496%2C31062297&oid=3&pvsid=3583215689260389&pem=501&eae=0&fc=896&brdim=22%2C45%2C22%2C45%2C1792%2C23%2C1200%2C973%2C1200%2C850&vis=1&rsz=%7C%7CeE%7C&abl=CS&pfx=0&fu=0&bc=31&ifi=1&uci=a!1&fsb=1&xpc=XZx1VYThaf&p=https%3A//www.zaubacorp.com&dtd=457',
    'https://www.googletagservices.com/activeview/js/current/osd.js',
    'https://googleads.g.doubleclick.net/pagead/ads?client=ca-pub-3139678088996753&output=html&adk=1812271804&adf=3025194257&lmt=1630248415&plat=9%3A32776%2C16%3A8388608%2C17%3A32%2C24%3A32%2C25%3A32%2C30%3A1048576%2C32%3A32&format=0x0&url=https%3A%2F%2Fwww.zaubacorp.com%2Fcompanybrowse&ea=0&flash=0&pra=7&wgl=1&uach=WyJtYWNPUyIsIjEwLjE1LjciLCJ4ODYiLCIiLCI5My4wLjQ1NzcuMCIsW10sbnVsbCxudWxsLCI2NCJd&tt_state=W3siaXNzdWVyT3JpZ2luIjoiaHR0cHM6Ly9hdHRlc3RhdGlvbi5hbmRyb2lkLmNvbSIsInN0YXRlIjo3fV0.&dt=1630248414949&bpp=1&bdt=930&idt=446&shv=r20210824&mjsv=m202108240101&ptt=9&saldr=aa&abxe=1&prev_fmts=728x90&nras=1&correlator=6198596691614&frm=20&pv=1&ga_vid=1664612034.1630248415&ga_sid=1630248415&ga_hid=1791176061&ga_fc=0&ga_wpids=UA-56482455-1&u_tz=330&u_his=2&u_java=0&u_h=1120&u_w=1792&u_ah=1017&u_aw=1792&u_cd=30&u_nplug=3&u_nmime=4&adx=-12245933&ady=-12245933&biw=1200&bih=850&scr_x=0&scr_y=0&eid=21067496%2C31062297&oid=3&pvsid=3583215689260389&pem=501&eae=2&fc=896&brdim=22%2C45%2C22%2C45%2C1792%2C23%2C1200%2C973%2C1200%2C850&vis=1&rsz=%7C%7Cs%7C&abl=NS&fu=32768&bc=31&ifi=3&uci=a!3&fsb=1&dtd=453',
    'https://pagead2.googlesyndication.com/pagead/managed/js/adsense/m202108240101/reactive_library_fy2019.js',
    'https://adservice.google.co.in/adsid/integrator.js?domain=www.zaubacorp.com',
    'https://adservice.google.com/adsid/integrator.js?domain=www.zaubacorp.com',
    'https://googleads.g.doubleclick.net/pagead/html/r20210824/r20110914/zrt_lookup.html?fsb=1',
    'https://pagead2.googlesyndication.com/getconfig/sodar?sv=200&tid=gda&tv=r20210824&st=env'
];

(async () => {
    function timeout(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    if (cluster.isMaster) {
        console.log(`Primary ${process.pid} is running`);

        const connections = new Array(15).fill(0)

        for (let connection of connections) {
            cluster.fork()
            await timeout(5000)
        }

        cluster.on('exit', (worker, code, signal) => {
            console.log(`worker ${worker.process.pid} died`);
        });
    } else {
        const connection = mysql.createConnection({
            host:'google-account.cmlfk75xsv3h.ap-south-1.rds.amazonaws.com', 
            user: 'shahrushabh1996', 
            database: 'rapidTax',
            password: '11999966',
            ssl: 'Amazon RDS'
        })

        const query = util.promisify(connection.query).bind(connection)

        let directorPages

        try {
            console.log('Transaction begin')

            await connection.beginTransaction()

            directorPages = await query(`SELECT * FROM directorPages WHERE status = 'NOT STARTED' ORDER BY id ASC LIMIT 1530`)

            console.log('directorPages Fetched')

            const directorPagesIds = []

            directorPages.map((directorPage) => directorPagesIds.push(directorPage.id))

            console.log('directorPagesIds array ready')

            if (directorPages.length) {
                await query(`UPDATE directorPages SET status = ? WHERE id IN (?)`, ['PROCESSING', directorPagesIds])
            }

            console.log('Updating status')

            await connection.commit()
        } catch (err) {
            console.log(err)
            connection.rollback()
        }

        const browser = await puppeteer.launch({
            defaultViewport: null,
            headless: true,
            args: ['--no-sandbox']
        });

        const page = await browser.newPage();

        await page.setRequestInterception(true);

        page.on('request', (req) => {
            // urls.push(req.url())
            // req.continue();
            if(blockedUrls.indexOf(req.url()) >= 0){
                req.abort();
            } else {
                req.continue();
            }
        });

        for (let directorPage of directorPages) {
            const t0 = performance.now()

            console.log(`${directorPage.alphabet} ::: ${directorPage.url} ::: Process Started`)

            try {
                await page.goto(directorPage.url);

                const directors = await page.evaluate(() => {
                    var $table = $("table")
                    var rows = []
                    var header = []
            
                    $table.find("tr th").each(function () {
                        header.push($(this).text().trim())
                    })
            
                    $table.find("tbody tr").each(function () {
                        var row = {}
            
                        $(this).find("td").each(function (i) {
                            row['Director'] = $(this).text().trim()
                            const link = $(this).find('a').attr('href')
                            const linkArr = link.split('/')
                            row['DIN'] = linkArr[5]
                        })
            
                        rows.push(row)
                    })

                    return rows
                })

                await query('INSERT INTO directors (id, Director, DIN) VALUES ?',
                [directors.map(director => [uuid(), director.Director, director.DIN])])

                await query(`UPDATE directorPages SET status = ? WHERE id IN (?)`, ['SUCCESS', directorPage.id])

                const t1 = performance.now()
                    
                console.log(`${directorPage.alphabet} ::: ${directorPage.url} ::: SUCCESS Time took ${((t1 - t0) / 1000)} seconds.`)
            } catch (err) {
                console.log(err)

                await query(`UPDATE directorPages SET status = ? WHERE id IN (?)`, ['FAILED', directorPage.id])

                const t1 = performance.now()
    
                console.log(`${directorPage.alphabet} ::: ${directorPage.url} ::: FAILED Time took ${((t1 - t0) / 1000)} seconds.`)
            }
        }
    }
})()