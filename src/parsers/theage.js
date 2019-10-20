//const feed_url = "https://www.theage.com.au/rss/politics/federal.xml";
const FEEDS = [
	{url: "https://www.theage.com.au/rss/politics/federal.xml", id: {publication: "theage", section: "federal"}},
	{url: "https://www.theage.com.au/rss/opinion.xml", id: {publication: "theage", section: "opinion"}},
	{url: "https://www.theage.com.au/rss/feed.xml", id: {publication: "theage", section: "headlines"}}
];

const feed_url = "https://www.theage.com.au/rss/feed.xml";
const Parser = require('rss-parser');
const uuidv5 = require('uuid/v5');
const {from} = require('rxjs');
const {flatMap, map, catchError, finalize, filter} = require('rxjs/operators');
const fetch = require('node-fetch');
const {parse: htmlParse} = require('node-html-parser');

const rssParser = new Parser();
const publication = "the age";
const {insert, find, insertIfNotExist, client} = require('../database/db');

const PUBLICATION = "c30371be-8b09-475c-8d57-d06bb2fe7311"; 

const rssFeed = from(FEEDS)
	.pipe(
		flatMap(async ({url, id}) => {
			return {
				id,
				feed: await rssParser.parseURL(url)
			};
		}),
		flatMap(ii => ii.feed.items.map(item => ({
				feedId: ii.id,
				...item
			}))
		),
		map(({link, pubDate, creator: author, title, feedId}) => {
			return {
				_id: uuidv5(link, uuidv5.URL),
				link,
				feedId,
				time_added: new Date(),
				pubDate: new Date(pubDate),
				author,
				title,
				publication: PUBLICATION
			}
		})
)


const fetchContent = () => (obs) => {
	return obs.pipe(
		flatMap(async (ii) => {
			return {
				...ii,
				html: await (await fetch(ii.link)).text()
			}
		})
	)
}

const parseContent = () => (obs) => {
	return obs.pipe(
		flatMap(async (ii) => {
			let root = htmlParse(ii.html);
			let article = root.querySelector("article");
			if(article != null){
				let sections = root.querySelector("article").querySelectorAll("section")
				let [content] = sections
					.reduce(( rr, ii, idx) => idx ? rr = `${rr} ${ii.text} ` : "", "")
					.replace("Loading", " ")
					.split("License this article");
				return {
					...ii,
					content
				}
			} else {
				console.log('no article');
				return null;
			}
		}),
		filter(ii => ii)

	)
}


async function parseArticles(){
	try{
		await client.connect();
		let result = await rssFeed.pipe(
			fetchContent(),
			parseContent(),
			insertIfNotExist({collection: 'article'}),
		).toPromise()
		console.log(result);
	} catch (ee){
		console.error(ee)
	}
	client.close();
}

parseArticles();