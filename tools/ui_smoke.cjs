// Optional browser smoke test; npm install --no-save playwright, then install Chromium.
const fs = require('fs');
const path = require('path');
const os = require('os');
const {spawn} = require('child_process');
const {chromium} = require(process.env.RUNLEDGER_PLAYWRIGHT_MODULE || 'playwright');
const root = path.resolve(__dirname, '..');
const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'runledger-ui-'));
const app = spawn(process.env.RUNLEDGER_PYTHON || 'python3', ['-m', 'runledger', '--db', path.join(directory, 'jobs.sqlite'), 'demo', '--port', '0'], {cwd: root});
let browser;
let output = '';
app.stdout.on('data', chunk => { output += chunk.toString(); });
app.stderr.on('data', chunk => { output += chunk.toString(); });
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
(async () => {
  let url;
  for (let i=0;i<60;i++) {
    url = output.match(/http:\/\/127\.0\.0\.1:\d+/)?.[0];
    if (url) break;
    if (app.exitCode !== null) throw Error(output);
    await delay(100);
  }
  if (!url) throw Error('Server startup timed out: '+output);
  browser = await chromium.launch({headless:true,args:['--no-sandbox']});
  const page = await browser.newPage({viewport:{width:1440,height:1050}});
  const errors=[]; page.on('pageerror',error=>errors.push(error.message));
  await page.goto(url);
  await page.waitForFunction(()=>document.getElementById('completed').textContent==='2');
  const {jobs}=await (await fetch(url+'/api/jobs')).json();
  const baseline=jobs.find(job=>job.idempotency_key.includes('baseline')).id;
  const candidate=jobs.find(job=>job.idempotency_key.includes('regressed')).id;
  await page.locator('#before').selectOption(baseline);
  await page.locator('#after').selectOption(candidate);
  await page.locator('#compare').click();
  await page.locator('#comparison pre').waitFor();
  const comparison=JSON.parse(await page.locator('#comparison pre').textContent());
  if(comparison.passed)throw Error('Regressed fixture should fail guard');
  fs.mkdirSync(path.join(root,'reports'),{recursive:true});
  await page.screenshot({path:path.join(root,'reports','dashboard.png'),fullPage:true});
  await page.getByText('Inspect',{exact:true}).first().click();
  await page.locator('#detail[open]').waitFor();
  await page.locator('#close').click();
  await page.locator('#baselineButton').click();
  await page.waitForFunction(()=>document.getElementById('completed').textContent==='3');
  await page.setViewportSize({width:390,height:1000});
  if(await page.evaluate(()=>document.documentElement.scrollWidth>window.innerWidth))throw Error('Mobile page overflows');
  await page.screenshot({path:path.join(root,'reports','dashboard-mobile.png'),fullPage:true});
  if(errors.length)throw Error(JSON.stringify(errors));
  console.log(JSON.stringify({dashboard:'passed',regression_guard:'passed',inspect:'passed',submit:'passed',mobile:'passed',browser_errors:errors}));
})().catch(error=>{console.error(error);process.exitCode=1}).finally(async()=>{
  if(browser)await browser.close();
  app.kill('SIGINT');
  for(let i=0;i<30&&app.exitCode===null;i++)await delay(100);
  if(app.exitCode===null)app.kill('SIGKILL');
  fs.rmSync(directory,{recursive:true,force:true});
});
