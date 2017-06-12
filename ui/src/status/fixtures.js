export const fixtureStatusPageCells = [
  {
    i: 'c-bar-graphs-fly',
    isWidget: false,
    x: 0,
    y: 0,
    w: 12,
    h: 4,
    name: 'Alerts – Last 30 Days – Aspiring Bar Graph',
    queries: [
      {
        query:
          'SELECT count("value") AS "count_value" FROM "chronograf"."autogen"."alerts" WHERE time > :dashboardTime: GROUP BY time(1d)',
        label: 'count.value',
        queryConfig: {
          database: 'chronograf',
          measurement: 'alerts',
          retentionPolicy: 'autogen',
          fields: [
            {
              field: 'value',
              funcs: ['count'],
            },
          ],
          tags: {},
          groupBy: {
            time: '1d',
            tags: [],
          },
          areTagsAccepted: false,
          rawText: null,
          range: null,
        },
      },
    ],
    type: 'line',
    links: {
      self: '/chronograf/v1/status/23/cells/c-bar-graphs-fly',
    },
  },
  {
    i: 'recent-alerts',
    isWidget: true,
    name: 'Recent Alerts',
    type: 'alerts',
    x: 0,
    y: 5,
    w: 6.5,
    h: 7,
  },
  {
    i: 'news-feed',
    isWidget: true,
    name: 'News Feed',
    type: 'news',
    x: 6.5,
    y: 5,
    w: 3,
    h: 7,
  },
  {
    i: 'getting-started',
    isWidget: true,
    name: 'Getting Started',
    type: 'guide',
    x: 9.5,
    y: 5,
    w: 2.5,
    h: 7,
  },
]

export const fixtureJSONFeed = {
  version: 'https://jsonfeed.org/version/1',
  title: 'Daring Fireball',
  home_page_url: 'https://daringfireball.net/',
  feed_url: 'https://daringfireball.net/feeds/json',
  author: {
    url: 'https://twitter.com/gruber',
    name: 'John Gruber',
  },
  icon: 'https://daringfireball.net/graphics/apple-touch-icon.png',
  favicon: 'https://daringfireball.net/graphics/favicon-64.png',
  items: [
    {
      title: 'Apple Design Awards 2017',
      date_published: '2017-06-07T21:18:23Z',
      date_modified: '2017-06-07T21:21:05Z',
      id: 'https://daringfireball.net/linked/2017/06/07/ada-2017',
      url: 'https://daringfireball.net/linked/2017/06/07/ada-2017',
      external_url: 'https://developer.apple.com/design/awards/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>No on-stage ceremony this year, but they <a href="https://m.imore.com/wwdc-2017-ada">held a reception</a> for the winners.</p>\n\n<div>\n<a  title="Permanent link to ‘Apple Design Awards 2017’"  href="https://daringfireball.net/linked/2017/06/07/ada-2017">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'The Talk Show Live From WWDC 2017',
      date_published: '2017-06-07T01:44:28Z',
      date_modified: '2017-06-07T01:49:26Z',
      id:
        'https://daringfireball.net/linked/2017/06/06/the-talk-show-live-2017',
      url:
        'https://daringfireball.net/linked/2017/06/06/the-talk-show-live-2017',
      external_url: 'https://www.instagram.com/p/BVBLlHfjBAc/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Marco Arment is providing <a href="http://atp.fm/live">a live audio stream at ATP&#8217;s website</a>.</p>\n\n<div>\n<a  title="Permanent link to ‘The Talk Show Live From WWDC 2017’"  href="https://daringfireball.net/linked/2017/06/06/the-talk-show-live-2017">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Bozoma Saint John Heads to Uber From Apple',
      date_published: '2017-06-06T20:43:37Z',
      date_modified: '2017-06-06T20:43:38Z',
      id: 'https://daringfireball.net/linked/2017/06/06/bozoma-saint-john-uber',
      url:
        'https://daringfireball.net/linked/2017/06/06/bozoma-saint-john-uber',
      external_url: 'https://techcrunch.com/2017/06/06/bozoma-saint-john/amp/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Ingrid Lunden, reporting for TechCrunch:</p>\n\n<blockquote>\n  <p>Last week, ahead of WWDC, there was a ripple of news when Axios\n<a href="https://www.axios.com/apple-music-executive-bozoma-saint-john-plans-to-leave-the-company-2430821417.html">discovered</a> that Bozoma Saint John &#8212; one of the more noticeable\nexecs at the company for being a woman of color, who led an Apple\nMusic demo at the previous year’s WWDC to some acclaim &#8212; was\nleaving Apple. Now TechCrunch has learned where she’s landing:\nshe’s going to Uber.</p>\n\n<p>We received the news via a tip, and have confirmed the appointment\nthrough multiple sources at Uber. The company, we understand,\nviews the appointment as important in helping “turn the tide on\nrecent issues.”</p>\n\n<p>As for what role she will be taking, that’s something we’re still\ntrying to figure out. We understand that Uber will be making more\ndetails public later. Saint John’s track record is in marketing &#8212;\nmost recently at Apple but also with a long stint at Pepsi, among\nother places.</p>\n</blockquote>\n\n<p>Did not see that one coming. Nice score for Uber.</p>\n\n<div>\n<a  title="Permanent link to ‘Bozoma Saint John Heads to Uber From Apple’"  href="https://daringfireball.net/linked/2017/06/06/bozoma-saint-john-uber">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Apple Removes Facebook and Twitter Integration From iOS 11',
      date_published: '2017-06-06T20:41:10Z',
      date_modified: '2017-06-06T20:41:12Z',
      id:
        'https://daringfireball.net/linked/2017/06/06/facebook-twitter-ios-11',
      url:
        'https://daringfireball.net/linked/2017/06/06/facebook-twitter-ios-11',
      external_url:
        'https://www.axios.com/apple-removes-facebook-and-twitter-integration-from-ios-11-2433996734.html',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>This special treatment for Twitter and Facebook always seemed a little weird to me. It feels right that in iOS 11 they&#8217;re just apps again.</p>\n\n<div>\n<a  title="Permanent link to ‘Apple Removes Facebook and Twitter Integration From iOS 11’"  href="https://daringfireball.net/linked/2017/06/06/facebook-twitter-ios-11">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Claim Chowder: HomePod Touchscreen',
      date_published: '2017-06-06T20:36:14Z',
      date_modified: '2017-06-06T20:36:15Z',
      id: 'https://daringfireball.net/linked/2017/06/06/homepod-claim-chowder',
      url: 'https://daringfireball.net/linked/2017/06/06/homepod-claim-chowder',
      external_url:
        'https://daringfireball.net/2017/05/when_the_scoops_run_dry',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Mark Gurman and Alex Webb, <a href="https://www.bloomberg.com/news/articles/2017-05-31/apple-said-to-ready-siri-speaker-in-bid-to-rival-google-amazon">in the report for Bloomberg</a> last week I wrote about a few days ago:</p>\n\n<blockquote>\n  <p>Ahead of Apple’s launch, the competition has upgraded their\nspeakers with support for making voice calls, while Amazon’s\ngained a touchscreen. Apple’s speaker won’t include such a screen,\naccording to people who have seen the product.</p>\n</blockquote>\n\n<p><a href="https://www.macrumors.com/2017/05/13/kuo-wwdc-10-5-ipad-siri-speaker/">Ming-Chi Kuo two weeks ago</a>:</p>\n\n<blockquote>\n  <p>We also believe this new product will come with a touch panel.</p>\n</blockquote>\n\n<p>HomePod has a touchscreen on top.</p>\n\n<div>\n<a  title="Permanent link to ‘Claim Chowder: HomePod Touchscreen’"  href="https://daringfireball.net/linked/2017/06/06/homepod-claim-chowder">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'The Upcoming iMac Pro Is Not the New Mac Pro',
      date_published: '2017-06-05T23:06:44Z',
      date_modified: '2017-06-05T23:06:45Z',
      id: 'https://daringfireball.net/linked/2017/06/05/imac-pro-mac-pro',
      url: 'https://daringfireball.net/linked/2017/06/05/imac-pro-mac-pro',
      external_url:
        'https://www.apple.com/newsroom/2017/06/imac-pro-most-powerful-mac-arrives-december/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Apple:</p>\n\n<blockquote>\n  <p>In addition to the new iMac Pro, Apple is working on a completely\nredesigned, next-generation Mac Pro architected for pro customers\nwho need the highest-end, high-throughput system in a modular\ndesign, as well as a new high-end pro display.</p>\n</blockquote>\n\n<div>\n<a  title="Permanent link to ‘The Upcoming iMac Pro Is Not the New Mac Pro’"  href="https://daringfireball.net/linked/2017/06/05/imac-pro-mac-pro">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'John Nack: ‘Drinks and Food Worth Knowing in San Jose’',
      date_published: '2017-06-05T22:02:55Z',
      date_modified: '2017-06-05T22:36:02Z',
      id: 'https://daringfireball.net/linked/2017/06/05/san-jose-nack',
      url: 'https://daringfireball.net/linked/2017/06/05/san-jose-nack',
      external_url:
        'http://jnack.com/blog/2017/06/04/drinks-food-worth-knowing-in-san-jose/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>John Nack:</p>\n\n<blockquote>\n  <p>With the help of a few friends, I’ve gathered some links to places\nworth checking out during WWDC and beyond.</p>\n</blockquote>\n\n<div>\n<a  title="Permanent link to ‘John Nack: &#8216;Drinks and Food Worth Knowing in San Jose&#8217;’"  href="https://daringfireball.net/linked/2017/06/05/san-jose-nack">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '[Sponsor] Field Notes',
      date_published: '2017-06-05T12:24:21-04:00',
      date_modified: '2017-06-05T12:24:22-04:00',
      id: 'https://daringfireball.net/feeds/sponsors/2017/06/field_notes',
      url: 'https://daringfireball.net/feeds/sponsors/2017/06/field_notes',
      external_url:
        'https://fieldnotesbrand.com/?utm_source=dffs&utm_medium=text&utm_campaign=1706-yopop',
      author: {
        name: 'Daring Fireball Department of Commerce',
      },
      content_html:
        '\n<p>Field Notes is offering two special kits for Father’s Day and they’re boxed up nice and ready to go. Each includes memo books, notebooks and a hand-screened card. One features a matte, black Space Pen and the other, a fine, limited-edition, hand-crafted rollerball pen.</p>\n\n<p>We can ship a kit to you pronto. Then just write a note on the enclosed card and give it to your Pop. Just just like that, you’re a hero.</p>\n\n<p>Or, tell us what you’d like to say, and we’ll write the note, then ship the box so it arrives at Dad’s just before Father’s Day. (USA only). Click, click, done.</p>\n\n<p><a href="https://fieldnotesbrand.com/?utm_source=dffs&amp;utm_medium=text&amp;utm_campaign=1706-yopop">These kits are only available until Tuesday, June 13th. Visit Field Notes today for all the details.</a></p>\n\n<p>Field Notes</p>\n\n<p>I’m not writing it down to remember it later, <br />\nI’m writing it down to remember it now.</p>\n\n<div>\n<a  title="Permanent link to ‘Field Notes’"  href="https://daringfireball.net/feeds/sponsors/2017/06/field_notes">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Jamf Now',
      date_published: '2017-06-04T00:48:00Z',
      date_modified: '2017-06-04T00:55:09Z',
      id: 'https://daringfireball.net/linked/2017/06/03/jamf-now',
      url: 'https://daringfireball.net/linked/2017/06/03/jamf-now',
      external_url:
        'https://www.jamf.com/lp/set-up-manage-and-protect-apple-devices-at-work/?utm_source=daringfireball&utm_medium=text&utm_campaign=2017-22',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>My thanks to Jamf for sponsoring this week&#8217;s DF feed.  Jamf Now is a simple, cloud-based solution designed to help anyone set up, manage, and protect Apple devices at work. Easily configure company email and Wi-Fi networks, distribute apps to your team, and protect sensitive data without locking down devices. Jamf Now allows you to treat IT as a task, not a full-time career.</p>\n\n<p><a href="https://www.jamf.com/lp/set-up-manage-and-protect-apple-devices-at-work/?utm_source=daringfireball&amp;utm_medium=text&amp;utm_campaign=2017-22">Daring Fireball readers can create an account and manage three devices for free</a>. Forever. Each additional device is just $2 per month. <a href="https://signup.jamfcloud.com/?utm_source=daringfireball&amp;utm_medium=text&amp;utm_campaign=2017-22">Create your free account today</a>.</p>\n\n<div>\n<a  title="Permanent link to ‘Jamf Now’"  href="https://daringfireball.net/linked/2017/06/03/jamf-now">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Ina Fried: Bozoma Saint John Plans to Leave Apple',
      date_published: '2017-06-04T00:15:00Z',
      date_modified: '2017-06-04T02:08:02Z',
      id: 'https://daringfireball.net/linked/2017/06/03/bozoma-saint-john',
      url: 'https://daringfireball.net/linked/2017/06/03/bozoma-saint-john',
      external_url:
        'https://www.axios.com/apple-music-executive-bozoma-saint-john-plans-to-leave-the-company-2430821417.html',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Ina Fried, reporting a scoop for Axios:</p>\n\n<blockquote>\n  <p>Bozoma Saint John, the Apple executive who garnered significant\nattention for her demo at last year&#8217;s worldwide developer\nconference, plans to leave the company, Axios has learned. Saint\nJohn was head of Global Consumer Marketing for Apple Music (and\npredecessor Beats Music). [&#8230;]</p>\n\n<p>While Apple has several women of color in higher-ranking\npositions, Saint John had a high profile beyond Apple and was\nwidely praised for her on-stage work last year. She was also\nfairly unique among Apple executives in maintaining a strong\npersonal brand beyond her work identity, with a strong following\non Instagram and Twitter.</p>\n</blockquote>\n\n<p>So much for <a href="https://daringfireball.net/thetalkshow/2017/05/27/ep-191">my prediction on The Talk Show</a> that we&#8217;d see Boz on-stage again in the WWDC keynote. I also enjoyed that the company with <a href="https://en.wikipedia.org/wiki/Steve_Wozniak">Woz</a> and <a href="https://www.imore.com/watch-greg-joswiaks-full-codemobile-interview-right-now">Joz</a> now had a Boz.</p>\n\n<div>\n<a  title="Permanent link to ‘Ina Fried: Bozoma Saint John Plans to Leave Apple’"  href="https://daringfireball.net/linked/2017/06/03/bozoma-saint-john">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '‘App: The Human Story’ Screening Tomorrow Night in San Jose',
      date_published: '2017-06-04T00:02:24Z',
      date_modified: '2017-06-04T00:02:27Z',
      id: 'https://daringfireball.net/linked/2017/06/03/app-the-human-story',
      url: 'https://daringfireball.net/linked/2017/06/03/app-the-human-story',
      external_url:
        'https://www.classy.org/san-jose/events/app-human-story-documentary-screening-presented-by-altconf-layers/e128096',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Tomorrow night in San Jose:</p>\n\n<blockquote>\n  <p>Join <a href="http://altconf.com/">AltConf</a> and <a href="https://layers.is/">Layers</a> on Sunday June 4 for an exclusive\npre-release screening of <a href="http://appdocumentary.com/">App: The Human Story</a>, a documentary that\ngives an intimate view into the journeys of independent app makers\nas they traverse a dynamic new industry. Following the screening,\na panel made up of cast members from the film, including Adam\nLisagor, Brent Simmons, Cabel Sasser, Grey Osten, John Gruber, Ish\nShabazz, Jay Dysart, Melissa Hargis, Steven Frank and Windy Chien,\nwill discuss the documentary film.</p>\n</blockquote>\n\n<p>Co-director Jake Schumacher will be there too. The screening is at 5p, and I&#8217;ll be leading the panel discussion afterward. I&#8217;ve seen a recent cut of the film and it&#8217;s terrific. I can&#8217;t believe this screening hasn&#8217;t sold out yet &#8212; get your tickets while there are still some left.</p>\n\n<p>Tickets are $25, and all proceeds go to <a href="http://appcamp4girls.com/">App Camp for Girls</a>. If you see me there, please say hello.</p>\n\n<div>\n<a  title="Permanent link to ‘&#8216;App: The Human Story&#8217; Screening Tomorrow Night in San Jose’"  href="https://daringfireball.net/linked/2017/06/03/app-the-human-story">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '‘New York Stories’',
      date_published: '2017-06-02T20:09:48Z',
      date_modified: '2017-06-02T20:09:49Z',
      id: 'https://daringfireball.net/linked/2017/06/02/new-york-times',
      url: 'https://daringfireball.net/linked/2017/06/02/new-york-times',
      external_url:
        'https://www.nytimes.com/interactive/2017/06/02/magazine/new-york-stories-introduction.html?_r=0',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>This week&#8217;s issue of The New York Times Magazine turned the entire magazine into a series of comics, illustrating stories from the newspaper&#8217;s Metro section. Even the crossword is hand-drawn. And the web version has some nifty animation.</p>\n\n<p>Fun.</p>\n\n<div>\n<a  title="Permanent link to ‘&#8216;New York Stories&#8217;’"  href="https://daringfireball.net/linked/2017/06/02/new-york-times">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '★ Update on The Talk Show Live From WWDC 2017',
      date_published: '2017-06-02T05:41:06Z',
      date_modified: '2017-06-02T06:13:18Z',
      id:
        'https://daringfireball.net/2017/06/update_on_the_talk_show_live_from_wwdc_2017',
      url:
        'https://daringfireball.net/2017/06/update_on_the_talk_show_live_from_wwdc_2017',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>On Wednesday I put the first 500 tickets on sale for next week&#8217;s live show from WWDC. They sold out in 7 minutes.</p>\n\n<p>The California Theatre in San Jose has both an orchestra level and a balcony. That first bunch of tickets separated the two. After talking with the staff at the theater today, they recommended making all tickets general admission and allowing their ushers to fill the orchestra level first, and then direct remaining ticket holders to the balcony. So, all tickets, including those sold Wednesday, are now simply general admission. Everyone paid the same price, so I think this is fair, but I do apologize for any confusion. The theater is beautiful, and there are no bad seats.</p>\n\n<p>The next batch of tickets <a href="https://ti.to/daringfireball/the-talk-show-WWDC-2017/">will go on sale today, Friday, at 1p ET/10a PT</a>. Given what happened Wednesday, I expect them to sell out in a few minutes. I hate writing that because it sounds braggy, but I&#8217;m putting it out there just as fair warning. You&#8217;re going to have to act quick and maybe get lucky.</p>\n\n<p>If you want a ticket and wind up not getting one, there will be a live audio stream for everyone to listen to. This year we are not going to attempt to stream live video. Instead we&#8217;re going to work hard to get edited video of the event up on the web as soon as possible after the show is over. If you just can&#8217;t wait, listen to the live audio. If you want to <em>see</em> the show, wait for the video &#8212; it should be up some time on Wednesday at the latest.</p>\n\n<p>If you do get a ticket or already have one:</p>\n\n<ul>\n<li>All seats are general admission, with no distinction between orchestra level and the balcony.</li>\n<li>Put your ticket in Apple Wallet and bring ID to show at the door. (If you don&#8217;t have an iPhone (?) bring a copy of the ticket PDF.)</li>\n<li>Doors open at 6p, and there will be an open bar. Find a seat, grab a beverage, and mingle with your fellow fans of the show.</li>\n<li>The show itself should start at 7p.</li>\n</ul>\n\n\n\n    ',
    },
    {
      title: 'Dave Hamilton on Apple’s Rumored Siri Speaker',
      date_published: '2017-06-02T03:59:00Z',
      date_modified: '2017-06-02T04:56:48Z',
      id: 'https://daringfireball.net/linked/2017/06/01/hamilton-siri-speaker',
      url: 'https://daringfireball.net/linked/2017/06/01/hamilton-siri-speaker',
      external_url:
        'https://www.macobserver.com/analysis/apple-siri-speaker-sonos/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Dave Hamilton, writing for The MacObserver:</p>\n\n<blockquote>\n  <p>All the reports of Apple’s rumored Siri Speaker have it targeting\nAlexa and Google Home, and at one level that makes sense. The Siri\nSpeaker seems like it will be a voice-controlled device in your\nhome that happens to be able to emit sound. But I think there’s a\ndifferent target Apple’s going after: <a href="https://www.macobserver.com/tmo/article/what-is-sonos">Sonos</a>. Remember, this isn’t\nrumored to just be a voice-controlled device with a speaker thrown\nin for audio feedback. Reports peg this as a device which contains\nmultiple, high-quality speakers. [&#8230;]</p>\n\n<p>I would expect a device with seven tweeters to provide truly\nroom-filling sound, perhaps from all angles given that each of\nthose tweeters could be aimed in slightly different directions.\nThis might be something that could pair with an Apple TV and not\nonly play music but also play the audio for your TV shows and\nmovies. The Siri Speaker may be more of a living room device than\na kitchen device.</p>\n</blockquote>\n\n<p>Man, this sounds like a great idea. Like Sonos but with really good AirPlay &#8212; like next-generation &#8220;AirPlay 2&#8221; AirPlay. Sonos doesn&#8217;t support AirPlay period.</p>\n\n<div>\n<a  title="Permanent link to ‘Dave Hamilton on Apple&#8217;s Rumored Siri Speaker’"  href="https://daringfireball.net/linked/2017/06/01/hamilton-siri-speaker">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Outsourcing Your Online Presence',
      date_published: '2017-06-02T03:45:48Z',
      date_modified: '2017-06-02T03:45:50Z',
      id: 'https://daringfireball.net/linked/2017/06/01/cieplinski-facebook',
      url: 'https://daringfireball.net/linked/2017/06/01/cieplinski-facebook',
      external_url:
        'http://www.joecieplinski.com/blog/2017/06/01/outsourcing-your-online-presence/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Joe Cieplinski:</p>\n\n<blockquote>\n  <p>Look, I get that I’m the nut who doesn’t want to use Facebook. I’m\nnot even saying don’t post your stuff to Facebook. But if Facebook\nis the <em>only</em> place you are posting something, know that you are\nshutting out people like me for no good reason. Go ahead and post\nto Facebook, but post it somewhere else, too. Especially if you’re\nrunning a business.</p>\n\n<p>The number of restaurants, bars, and other local establishments\nthat, thanks to crappy web sites they can’t update, post their\ndaily specials, hours, and important announcements only via\nFacebook is growing. That’s maddening. Want to know if we’re open\nthis holiday weekend? Go to Facebook.</p>\n\n<p>Go to hell.</p>\n</blockquote>\n\n<div>\n<a  title="Permanent link to ‘Outsourcing Your Online Presence’"  href="https://daringfireball.net/linked/2017/06/01/cieplinski-facebook">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title:
        'A Metaphor for How Cultural, Technological, and Scientific Changes Happen',
      date_published: '2017-06-02T03:00:34Z',
      date_modified: '2017-06-02T03:01:00Z',
      id: 'https://daringfireball.net/linked/2017/06/01/dominoes',
      url: 'https://daringfireball.net/linked/2017/06/01/dominoes',
      external_url:
        'http://kottke.org/17/06/this-is-a-metaphor-for-how-cultural-technological-and-scientific-changes-happen',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Proof that small efforts can lead to big results.</p>\n\n<div>\n<a  title="Permanent link to ‘A Metaphor for How Cultural, Technological, and Scientific Changes Happen’"  href="https://daringfireball.net/linked/2017/06/01/dominoes">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'On Giving a Shit',
      date_published: '2017-06-02T02:59:59Z',
      date_modified: '2017-06-02T04:05:58Z',
      id: 'https://daringfireball.net/linked/2017/06/01/on-giving-a-shit',
      url: 'https://daringfireball.net/linked/2017/06/01/on-giving-a-shit',
      external_url: 'https://twitter.com/joehewitt/status/870363197580038144',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Joe Hewitt, possibly in response to <a href="http://scripting.com/2017/05/31.html#a110526">Dave Winer&#8217;s</a> and <a href="https://daringfireball.net/2017/06/fuck_facebook">my</a> objections to Facebook today:</p>\n\n<blockquote>\n  <p>Seriously guys, nobody gives a shit about the open web. Only your clique.</p>\n</blockquote>\n\n<p>A few thoughts:</p>\n\n<ul>\n<li><p>Most people don&#8217;t care about &#8220;the open web&#8221; at the technical or political (and in my personal case, business) level that Dave Winer and I do. Most people, I&#8217;m sure, couldn&#8217;t even offer a cogent definition of what &#8220;the open web&#8221; means. Nor should they have to. They just know they can open a web browser, search for things, visit their favorite sites, and click links from one site to another. But I&#8217;ll tell you what: I bet most people think it sucks that stuff posted publicly to Facebook &#8212; like Marc Haynes&#8217;s lovely story about Roger Moore &#8212; can&#8217;t be searched by Google. And I bet they&#8217;d be pissed if they knew that it wasn&#8217;t a technical issue on Google&#8217;s side but simply a deliberate strategic decision by Facebook. People may not know what the open web is but they like it.</p></li>\n<li><p>What a sad way to go through life, discouraging people from fighting for what they know to be both right and good for the world, simply because most people may not understand. &#8220;<em>Just give up</em>&#8221; seems to be Hewitt&#8217;s advice.</p></li>\n<li><p><a href="https://techcrunch.com/2009/11/11/joe-hewitt-developer-of-facebooks-massively-popular-iphone-app-quits-the-project/">Joe Hewitt in 2009</a>:</p>\n\n<blockquote>\n  <p>The web is still unrestricted and free, and so I am returning to\nmy roots as a web developer. In the long term, I would like to be\nable to say that I helped to make the web the best mobile platform\navailable, rather than being part of the transition to a world\nwhere every developer must go through a middleman to get their\nsoftware in the hands of users.</p>\n</blockquote></li>\n</ul>\n\n<div>\n<a  title="Permanent link to ‘On Giving a Shit’"  href="https://daringfireball.net/linked/2017/06/01/on-giving-a-shit">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Silicon Valley vs. Wall Street',
      date_published: '2017-06-02T02:24:46Z',
      date_modified: '2017-06-02T02:24:47Z',
      id:
        'https://daringfireball.net/linked/2017/06/01/silicon-valley-vs-wall-street',
      url:
        'https://daringfireball.net/linked/2017/06/01/silicon-valley-vs-wall-street',
      external_url:
        'https://www.bloomberg.com/view/articles/2017-05-25/ethics-quants-and-cold-calling',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Matt Levine, in a fascinating and wide-ranging column for Bloomberg:</p>\n\n<blockquote>\n  <p>One of the most incredible <a href="https://twitter.com/antoniabmassa/status/867484927096475648">feats of marketing</a> of our century is\nthat the internet companies have convinced a lot of people that\nselling advertisements on web pages is basically the same as\ncuring cancer, while buying stocks and bonds is evil:</p>\n\n<blockquote>\n  <p>“At tech companies, the permeating value is that they’re about\ntrying to make the world a better place, whereas at hedge funds\nit’s about making more money,” Mr. Epstein said.</p>\n</blockquote>\n\n<p>That&#8217;s from a <a href="https://www.wsj.com/articles/battle-royale-hedge-funds-vs-silicon-valley-1495637466">Wall Street Journal article</a> &#8212; in its series on\nquants &#8212; about the talent battle between Wall Street and Silicon\nValley. As far as I can tell, the pitch for data scientists from\nSilicon Valley is: &#8220;Come work here, you can build advertising\nmodels and pretend that you&#8217;re saving the world,&#8221; while the pitch\nfor data scientists from Wall Street is: &#8220;Come work here, you can\nbuild trading models and not have to pretend that you&#8217;re saving\nthe world.&#8221; I actually think that is a useful sorting metric, and\nI know which one I would take.</p>\n</blockquote>\n\n<div>\n<a  title="Permanent link to ‘Silicon Valley vs. Wall Street’"  href="https://daringfireball.net/linked/2017/06/01/silicon-valley-vs-wall-street">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title:
        '‘Climate Change Is Real’: U.S. Companies Lament Paris Accord Exit',
      date_published: '2017-06-02T02:14:56Z',
      date_modified: '2017-06-02T03:31:33Z',
      id: 'https://daringfireball.net/linked/2017/06/01/climate-change-trump',
      url: 'https://daringfireball.net/linked/2017/06/01/climate-change-trump',
      external_url:
        'https://mobile.nytimes.com/2017/06/01/business/climate-change-tesla-corporations-paris-accord.html',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Daniel Victor, writing for The New York Times:</p>\n\n<blockquote>\n  <p>Twenty-five companies, including Apple, Facebook, Google and\nMicrosoft, bought full-page ads in The New York Times, The Wall\nStreet Journal and The New York Post last month to argue their\ncase. Some of those companies, and others with similar views in\nthe technology, energy and engineering sectors, reacted quickly on\nThursday.</p>\n</blockquote>\n\n<p>Apple, Google, Facebook, Microsoft, Tesla, Twitter, GE, Goldman Sachs &#8212; the leaders of all these companies spoke out against Trump&#8217;s moronic decision to withdraw from the Paris agreement. Even Shell and Exxon wanted the U.S. to remain in the agreement. The only CEO the Times quoted who supports this nonsensical decision is from a fucking coal company.</p>\n\n<p>197 countries agreed to the Paris Accord. Prior to today&#8217;s U.S. withdrawal, only Syria and Nicaragua weren&#8217;t in &#8212; Syria isn’t in because they were in the midst of a brutal civil war at the time, and Nicaragua refused to sign only because they felt the accord didn&#8217;t go far enough. Every major captain of industry in the U.S. outside the coal industry publicly asked Trump to keep the U.S. in the Paris Accord. It’s good for business and good for the environment.</p>\n\n<p>The United States stands utterly alone on this. Trump has put the United States on the fringes of civilization.</p>\n\n<div>\n<a  title="Permanent link to ‘‘Climate Change Is Real’: U.S. Companies Lament Paris Accord Exit’"  href="https://daringfireball.net/linked/2017/06/01/climate-change-trump">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title:
        'Swift Playgrounds Expands Coding Education to Robots, Drones and Musical Instruments',
      date_published: '2017-06-02T01:43:14Z',
      date_modified: '2017-06-02T01:43:15Z',
      id:
        'https://daringfireball.net/linked/2017/06/01/swift-playgrounds-robots',
      url:
        'https://daringfireball.net/linked/2017/06/01/swift-playgrounds-robots',
      external_url:
        'https://www.apple.com/newsroom/2017/06/swift-playgrounds-expands-coding-education-to-robots-drones-and-musical-instruments/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Apple:</p>\n\n<blockquote>\n  <p>Apple today announced that Swift Playgrounds, its educational\ncoding app for iPad, will offer an exciting new way to learn to\ncode using robots, drones and musical instruments. Swift\nPlaygrounds is perfect for students and beginners learning to code\nwith Swift, Apple’s powerful and intuitive programming language\nfor building world-class apps. Apple is working with leading\ndevice makers to make it easy to connect to Bluetooth-enabled\nrobots within the Swift Playgrounds app, allowing kids to program\nand control popular devices, including LEGO MINDSTORMS Education\nEV3, the Sphero SPRK+, Parrot drones and more. The Swift\nPlaygrounds 1.5 update will be available as a free download on the\nApp Store beginning Monday, June 5.</p>\n</blockquote>\n\n<p>This is a very cool announcement in and of itself. But my first thought after seeing this announced <em>today</em> was that Monday&#8217;s keynote must be jam-packed. This could have easily been a 10-minute segment in the keynote, with cool on-stage demos.</p>\n\n<div>\n<a  title="Permanent link to ‘Swift Playgrounds Expands Coding Education to Robots, Drones and Musical Instruments’"  href="https://daringfireball.net/linked/2017/06/01/swift-playgrounds-robots">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Pinboard Acquires Delicious',
      date_published: '2017-06-01T19:01:58Z',
      date_modified: '2017-06-01T19:02:17Z',
      id:
        'https://daringfireball.net/linked/2017/06/01/pinboard-acquires-delicious',
      url:
        'https://daringfireball.net/linked/2017/06/01/pinboard-acquires-delicious',
      external_url:
        'https://blog.pinboard.in/2017/06/pinboard_acquires_delicious/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>This is simply amazing given the <a href="https://blog.pinboard.in/2011/03/anatomy_of_a_crushing/">history of Pinboard</a>. Strike a win for the indie web.</p>\n\n<div>\n<a  title="Permanent link to ‘Pinboard Acquires Delicious’"  href="https://daringfireball.net/linked/2017/06/01/pinboard-acquires-delicious">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title:
        'Putin Hints at U.S. Election Meddling by ‘Patriotically Minded’ Russians',
      date_published: '2017-06-01T18:47:00Z',
      date_modified: '2017-06-01T18:47:19Z',
      id: 'https://daringfireball.net/linked/2017/06/01/putin-trump',
      url: 'https://daringfireball.net/linked/2017/06/01/putin-trump',
      external_url:
        'https://www.nytimes.com/2017/06/01/world/europe/vladimir-putin-donald-trump-hacking.html',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Andrew Higgins, reporting for The New York Times:</p>\n\n<blockquote>\n  <p>Shifting from his previous blanket denials, President Vladimir\nV. Putin of Russia said on Thursday that “patriotically minded”\nprivate Russian hackers could have been involved in\ncyberattacks last year to help the presidential campaign of\nDonald J. Trump. [&#8230;]</p>\n\n<p>Raising the possibility of attacks by what he portrayed as\nfree-spirited Russian patriots, Mr. Putin said that hackers “are\nlike artists” who choose their targets depending how they feel\n“when they wake up in the morning. If they are patriotically\nminded, they start making their contributions &#8212; which are right,\nfrom their point of view &#8212; to the fight against those who say bad\nthings about Russia.” [&#8230;]</p>\n\n<p>Perhaps worried that American intelligence agencies could release\nevidence linking last year’s cyberattacks to Russia, Mr. Putin\nalso put forward a theory that modern technology could easily be\nmanipulated to create a false trail back to Russia.</p>\n</blockquote>\n\n<p><a href="https://twitter.com/aodespair/status/870288487244718081">David Simon nails it</a>:</p>\n\n<blockquote>\n  <p>This is a ridiculous fallback position, meaning: Cat&#8217;s out of the\nbag. We fucked up your election and left evidence.</p>\n</blockquote>\n\n<div>\n<a  title="Permanent link to ‘Putin Hints at U.S. Election Meddling by ‘Patriotically Minded’ Russians’"  href="https://daringfireball.net/linked/2017/06/01/putin-trump">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '★ Fuck Facebook',
      date_published: '2017-06-01T17:56:29Z',
      date_modified: '2017-06-01T19:57:42Z',
      id: 'https://daringfireball.net/2017/06/fuck_facebook',
      url: 'https://daringfireball.net/2017/06/fuck_facebook',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Dave Winer, &#8220;<a href="http://scripting.com/2017/05/31.html#a110526">Why I Can&#8217;t/Won&#8217;t Point to Facebook Blog Posts</a>&#8221;:</p>\n\n<blockquote>\n  <p>1. It&#8217;s impractical. I don&#8217;t know what your privacy settings are.\n   So if I point to your post, it&#8217;s possible a lot of people might\n   not be able to read it, and thus will bring the grief to me,\n   not you, because they have no idea who you are or what you\n   wrote.</p>\n\n<p>2. It&#8217;s supporting their downgrading and killing the web. Your\n   post sucks because it doesn&#8217;t contain links, styling, and you\n   can&#8217;t enclose a podcast if you want. The more people post\n   there, the more the web dies. I&#8217;m sorry no matter how good\n   your idea is fuck you I won&#8217;t help you and Facebook kill the\n   open web.</p>\n</blockquote>\n\n<p>I&#8217;ve made exceptions a handful of times over the years, but as a general rule I refuse to link to anything on Facebook either, for the same reasons as Dave. Last week <a href="https://daringfireball.net/linked/2017/05/23/meeting-roger-moore">I linked to <em>screenshots</em> of a Facebook post</a> to avoid linking to the original. <a href="https://www.facebook.com/marc.haynes.7583/posts/793204930854561?match=bWFyYyBoYXluZXMscm9nZXIgbW9vcmU%3D">The original post</a> by Marc Haynes was public, which I know because I do not have a Facebook account, but <a href="https://daringfireball.net/misc/2017/06/haynes-facebook.png">here&#8217;s what it looks like for me</a> without being a Facebook user &#8212; a full one-third of my window is covered by a pop-over trying to get me to sign in or sign up for Facebook. I will go out of my way to avoid linking to websites that are hostile to users with pop-overs. (For example, I&#8217;ve largely stopped linking to anything from Wired, because they have such an aggressive anti-ad-block detection scheme. Fuck them.)</p>\n\n<p>You might think it&#8217;s hyperbole for Winer to say that Facebook is trying to kill the open web. But they are. I complain about Google AMP, but AMP is just a dangerous step toward a Google-owned walled garden &#8212; Facebook is designed from the ground up as an all-out attack on the open web. Marc Haynes&#8217;s Facebook post about Roger Moore is viewable by anyone, but:</p>\n\n<p><em>It is not accessible to search engines.</em> Search for &#8220;Marc Haynes Roger Moore&#8221; on any major search engine &#8212; <a href="https://duckduckgo.com/?q=marc+haynes+roger+moore&amp;ia=news">DuckDuckGo</a>, <a href="https://encrypted.google.com/search?hl=en&amp;q=marc%20haynes%20roger%20moore">Google</a>, <a href="https://www.bing.com/search?q=marc%20haynes%20roger%20moore">Bing</a> &#8212; and you will get hundreds of results. The story went viral, deservedly. But not only is the top result <em>not</em> Haynes&#8217;s original post on Facebook, his post doesn&#8217;t show up <em>anywhere</em> in the results because Facebook forbids search engines from indexing Facebook posts. Content that isn&#8217;t indexable by search engines is not part of the open web. (Even if I wanted to link to Haynes&#8217;s original post, how was I supposed to find it? I wound up with the original post URL via a Facebook-using friend who knows I prefer to link to original posts as a general rule.) The only way to find Facebook posts is through Facebook.</p>\n\n<p>Winer&#8217;s third reason:</p>\n\n<blockquote>\n  <p>3. Facebook might go out of business. I like to point to things\nthat last. Facebook seems solid now, but they could go away or\nretire the service you posted on. Deprecate the links. Who knows.\nYou might not even mind, but I do. I like my archives to last as\nlong as possible.</p>\n</blockquote>\n\n<p>Facebook going out of business seems unlikely. But Facebook pulling a Vader and <a href="https://www.youtube.com/watch?v=WpE_xMRiCLE">altering the deal</a>, blocking public access in the future to a post that today is publicly visible? It wouldn&#8217;t surprise me if it happened tomorrow. And in the same way they block indexing by search engines, <a href="https://web.archive.org/web/*/https://www.facebook.com/marc.haynes.7583/posts/793204930854561?match=bWFyYyBoYXluZXMscm9nZXIgbW9vcmU%3D">Facebook forbids The Internet Archive from saving copies of posts</a>.</p>\n\n<p>The Internet Archive is our only good defense against broken links. Blocking them from indexing Facebook content is a huge &#8220;fuck you&#8221; to anyone who cares about the longevity of the stuff they link to.</p>\n\n<p>Treat Facebook as the private walled garden that it is. If you want something to be publicly accessible, post it to a real blog on any platform that embraces the real web, the open one.</p>\n\n\n\n    ',
    },
    {
      title: 'The Talk Show: ‘The Original Sin Is XML’',
      date_published: '2017-06-01T03:59:00Z',
      date_modified: '2017-06-01T19:54:18Z',
      id: 'https://daringfireball.net/linked/2017/05/31/the-talk-show-192',
      url: 'https://daringfireball.net/linked/2017/05/31/the-talk-show-192',
      external_url: 'https://daringfireball.net/thetalkshow/2017/05/31/ep-192',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Manton Reece and whisky-soaked baritone Brent Simmons join the show to talk about JSON Feed, the new spec they co-authored for syndicating things like blog posts and podcasts. We talk about their longstanding mutual interest in <a href="http://frontier.userland.com">Userland Frontier</a> &#8212; Dave Winer’s groundbreaking scripting environment from the early ’90s &#8212; and how that background and their mutual love for publishing on the open web and the democratization of technology ultimately led to the creation of JSON Feed, as well as their other new projects: Manton’s <a href="https://micro.blog/">Micro.blog</a> publishing platform, and Brent’s new open source Mac app, announced for the first time right here on the show. And of course a brief look ahead to next week’s WWDC 2017.</p>\n\n<p>Brought to you by these fine sponsors:</p>\n\n<ul>\n<li><a href="http://awaytravel.com/talkshow">Away</a>: High quality luggage with built-in USB chargers. Save $20 with promo code <strong>TALKSHOW</strong>.</li>\n<li><a href="http://mailroute.net/tts">MailRoute</a>: Hosted spam and virus protection for email. Use this link for 10 percent off for the life of your account.</li>\n<li><a href="https://www.squarespace.com/thetalkshow">Squarespace</a>: Make your next move with a beautiful website. Use code <strong>gruber</strong> for 10 percent off your first order.</li>\n<li><a href="https://www.fractureme.com/podcast">Fracture</a>: Your photos printed in vivid color directly on glass. Great gift idea.</li>\n</ul>\n\n<div>\n<a  title="Permanent link to ‘The Talk Show: &#8216;The Original Sin Is XML&#8217;’"  href="https://daringfireball.net/linked/2017/05/31/the-talk-show-192">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '★ When the Scoops Run Dry',
      date_published: '2017-06-01T02:16:18Z',
      date_modified: '2017-06-01T04:59:54Z',
      id: 'https://daringfireball.net/2017/05/when_the_scoops_run_dry',
      url: 'https://daringfireball.net/2017/05/when_the_scoops_run_dry',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>800-word report for Bloomberg by Mark Gurman and Alex Webb on Apple&#8217;s long-rumored Siri speaker product, <a href="https://www.bloomberg.com/news/articles/2017-05-31/apple-said-to-ready-siri-speaker-in-bid-to-rival-google-amazon">with one sentence of actual news</a>:</p>\n\n<blockquote>\n  <p>The iPhone-maker has started manufacturing a long-in-the-works\nSiri-controlled smart speaker, according to people familiar with\nthe matter.</p>\n</blockquote>\n\n<p>Seriously, that&#8217;s about it for news about the product.</p>\n\n<p>Next sentence:</p>\n\n<blockquote>\n  <p>Apple could debut the speaker as soon as its annual developer\nconference in June, but the device will not be ready to ship until\nlater in the year, the people said.</p>\n</blockquote>\n\n<p>The keynote is five days away and Gurman and Webb don&#8217;t know if it&#8217;s going to be announced.</p>\n\n<blockquote>\n  <p>The device will differ from Amazon.com Inc.’s Echo and Alphabet\nInc.’s Google Home speakers by offering virtual surround sound\ntechnology and deep integration with Apple’s product lineup, said\nthe people, who requested anonymity to discuss products that\naren’t yet public.</p>\n</blockquote>\n\n<p>The &#8220;virtual surround sound&#8221; feature is arguably news, but there&#8217;s not one word about what &#8220;virtual surround sound&#8221; means. They could have at least <a href="https://en.wikipedia.org/wiki/Virtual_surround">linked to Wikipedia</a>. And Ming-Chi Kuo had a report with details of the device&#8217;s &#8220;<a href="https://www.macrumors.com/2017/05/01/apple-siri-smart-speaker-kgi-wwdc/">excellent acoustics performance (one woofer + seven tweeters)</a>&#8221; a month ago.</p>\n\n<p>Gurman and Webb:</p>\n\n<blockquote>\n  <p>Inventec Corp., the Taipei manufacturer that already makes the\nAirPod wireless headphones, will add the speaker to its Apple\nrepertoire, the people said. Apple employees have been secretly\ntesting the device in their homes for several months, they said.\nThe Siri speaker reached an advanced prototype stage late last\nyear, Bloomberg News reported at the time.</p>\n</blockquote>\n\n<p>What does it look like? How big is it? <del>Does it have a display?</del> <em>Crickets.</em></p>\n\n<p>[<strong>Update:</strong> I missed this sentence at the end of Gurman and Webb&#8217;s report: &#8220;Apple’s speaker won’t include such a screen, according to people who have seen the product.&#8221; That sets up a delicious claim chowder standoff with Ming-Chi Kuo, <a href="https://www.macrumors.com/2017/05/13/kuo-wwdc-10-5-ipad-siri-speaker/">who wrote two weeks ago</a>, &#8220;We also believe this new product will come with a touch panel.&#8221;]</p>\n\n<blockquote>\n  <p>Apple has also considered including sensors that measure a room’s\nacoustics and automatically adjust audio levels during use, one of\nthe people said.</p>\n</blockquote>\n\n<p>What did Apple decide about those sensors? If the device is in manufacturing, and the device might be announced in five days, one presumes Apple has made that decision, no? <em>Crickets.</em></p>\n\n<p>&#8220;<em>Apple&#8217;s long-rumored Siri-driven HomeKit speaker hub has entered manufacturing in Taipei</em>&#8221; &#8212; there&#8217;s a 13-word summary with all the actual news in this story. I like Mark Gurman, but it&#8217;s painful to see these meager <a href="https://www.macrumors.com/2017/05/01/apple-siri-smart-speaker-kgi-wwdc/">stale</a> morsels stretched into feature articles.</p>\n\n<p>AirPods &#8212; now <a href="https://9to5mac.com/2016/01/08/iphone-7-wireless-headphones-beats/"><em>that</em> was a scoop</a>. Nine months before Apple unveiled them, Gurman had an accurate description of how they worked, the charging case, and even had the &#8220;AirPods&#8221; name (albeit with a &#8220;which may be what these are called&#8221; caveat, and with the wrong assumption that AirPods would be Beats-branded rather than Apple-branded). With this Siri speaker dingus, he&#8217;s a month behind and has far fewer details than either <a href="https://www.macrumors.com/2017/05/13/kuo-wwdc-10-5-ipad-siri-speaker/">Ming-Chi Kuo</a> or <a href="https://twitter.com/SonnyDickson/status/857584682677829633">Sonny Dickson</a>.</p>\n\n<p>The closer we get to the WWDC keynote, the more likely things are <a href="https://9to5mac.com/2013/06/09/what-ios7-looks-like/" title="Seth Weintraub’s preview of iOS 7 a few days before it was announced at WWDC 2013.">to get spoiled</a>. But here we are 5 days out and no one has leaked just about anything about iOS 11 or MacOS 10.13, or what&#8217;s going on with this 10.5-inch iPad Pro, or if there&#8217;s anything new coming for WatchOS or tvOS. Again, there&#8217;s a lot of time between now and Monday morning, but it might be time to give Tim Cook credit for &#8220;<a href="https://www.theverge.com/2012/5/29/3051521/tim-cook-apple-will-double-down-on-secrecy-on-products">doubling down on secrecy</a>&#8221;.</p>\n\n\n\n    ',
    },
    {
      title:
        'Pierce Brosnan Pays Tribute to Roger Moore: ‘A Magnificent Actor’',
      date_published: '2017-05-31T21:56:50Z',
      date_modified: '2017-06-01T02:36:59Z',
      id: 'https://daringfireball.net/linked/2017/05/31/moore-brosnan',
      url: 'https://daringfireball.net/linked/2017/05/31/moore-brosnan',
      external_url:
        'http://variety.com/2017/film/news/pierce-brosnan-roger-moore-tribute-1202446432/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Pierce Brosnan:</p>\n\n<blockquote>\n  <p>Sean Connery had set the bar high, and George Lazenby, with mighty\nflair and a valiant heart, had given it his best. Now it was\nRoger’s turn. He knew his time was now, and he reigned over seven\nmovies as James Bond with exceptional skill and comic timing laced\nwith a stiletto vengeance. He knew his comedy, he knew who he was\nand he played onstage and off with an easy grace and charm. He\nknew that we knew.</p>\n</blockquote>\n\n<p>&#8220;He knew that we knew&#8221; is the phrase I&#8217;ve been searching for for years to describe Roger Moore&#8217;s take on Bond. Just perfect.</p>\n\n<div>\n<a  title="Permanent link to ‘Pierce Brosnan Pays Tribute to Roger Moore: ‘A Magnificent Actor’’"  href="https://daringfireball.net/linked/2017/05/31/moore-brosnan">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '★ What If the iPad Smart Keyboard Had a Trackpad?',
      date_published: '2017-05-31T20:53:19Z',
      date_modified: '2017-06-01T02:24:55Z',
      id: 'https://daringfireball.net/2017/05/ipad_trackpad',
      url: 'https://daringfireball.net/2017/05/ipad_trackpad',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Here&#8217;s an idea I tossed out <a href="https://daringfireball.net/thetalkshow/2017/05/27/ep-191">on the latest episode of The Talk Show</a>, while talking with Jim Dalrymple about what Apple might do with the iPad Pro: what if they added a trackpad to the Smart Keyboard? <a href="https://twitter.com/DaveChap/status/869192781960499200">David Chapman took the idea and made a quick mockup of what it might look like</a>. (I think the trackpad should be smaller than in his mockup &#8212; more like an older MacBook than a new MacBook, but his image conveys the general idea.)</p>\n\n<p>I&#8217;m <em>not</em> talking about adding an on-screen mouse cursor to iOS for clicking and dragging. That&#8217;s a terrible idea. My loose idea for an iPad trackpad is based on a few things:</p>\n\n<ol>\n<li><p>iOS&#8217;s trackpad-like mode for using the on-screen keyboard to move the insertion point around like a mouse cursor while editing text. A lot of people don&#8217;t know about this feature, and some who do misunderstand it, but it&#8217;s one of my favorite additions to iOS in recent years. If your iPhone has 3D Touch, while editing text you can hard press on the keyboard to turn it into a trackpad for moving the insertion point around in the text editing area. While in that mode, you can hard press again to change from moving the insertion point to selecting words (like double-click-then-drag on MacOS). iPads don’t (yet?) have 3D Touch, but you can access the same mode by putting two fingers on the on-screen keyboard and dragging. Two-finger touch on the keyboard and drag right away: move the insertion point. Two-finger touch on the keyboard, wait a moment for the insertion point to change to a barbell, and then drag: select words.</p></li>\n<li><p>tvOS&#8217;s UIFocusEngine. That&#8217;s the interface framework that allows Apple TV to be controlled by a trackpad or game controller <em>without</em> an on-screen mouse cursor. On Apple TV, you don&#8217;t move a cursor around, you move the selection around. <a href="https://daringfireball.net/linked/2015/11/13/uifocusengine-ios">Two years ago Steven Troughton-Smith discovered that an incomplete version of UIFocusEngine was built into iOS 9</a>.</p></li>\n<li><p>As things stand today while using an iPad with any hardware keyboard, selecting text and moving the insertion point around stinks. Yes, you can use the arrow keys on the keyboard (along with shortcuts like Option to move word-by-word instead of character-by-character, and Shift to select as you go), but it seems like a regression to 1983 to encourage an entirely keyboard-based routine for text editing. <a href="http://geekfun.com/2010/04/21/the-ipad-and-why-the-original-mac-didnt-have-arrow-keys/">The original Mac didn&#8217;t even have arrow keys on the keyboard</a>, to force users to use the mouse for moving the insertion point.</p></li>\n</ol>\n\n<p>In short, when you&#8217;re using the iPad&#8217;s on-screen keyboard, you have a crummy (or at the very least sub-par) keyboard for typing but a nice interface for moving the insertion point around. When you&#8217;re using the Smart Keyboard (or any other hardware keyboard) you have a decent keyboard for typing but no good way to move the insertion point or select text. Using your finger to touch the screen is imprecise, and, when an iPad is propped up laptop-style, ergonomically undesirable.</p>\n\n<p>A hardware keyboard with a trackpad could have just as good an interface for moving the insertion point and selecting text as the software keyboard. Even better, really, since you wouldn&#8217;t have to use two fingers or start it with a 3D Touch force press. And, a trackpad would make this feature discoverable. An awful lot of iPad owners &#8212; most of them, probably &#8212; don&#8217;t know about the two-finger drag feature on the on-screen keyboard.</p>\n\n<p>When you&#8217;re <em>not</em> editing text, the trackpad might not do much on an iPad. But the entire point of the smart keyboard is that you&#8217;re writing and editing text while it&#8217;s connected, or you&#8217;re just using it to prop up the iPad for watching video or something. But I think the trackpad could be used for selecting things or changing input focus. On the home screen you could use the trackpad to select an app to launch, just like on Apple TV. In split-screen multitasking mode, you could use a multitouch gesture on the trackpad to switch which pane has focus. Two-finger drags on the trackpad could scroll the current view, much like on the Mac.</p>\n\n<p>I fully admit this is not a perfect idea. But I do think it would greatly improve the efficiency of text editing on an iPad, and if text editing isn&#8217;t an essential task for iPad users, I don&#8217;t understand why Apple bothered making the Smart Keyboard in the first place. And, unlike adding touchscreen support to MacOS, adding trackpad support to iOS would not harm anything that is good about the way things already are.</p>\n\n<p>The biggest problem with this entire notion is that the way the Smart Keyboard folds from a cover to a keyboard would have to be redesigned, because the current design leaves no room at all for a trackpad. What I&#8217;m proposing might only be possible with a hard (non-folding) keyboard cover.</p>\n\n<hr />\n\n<p><a href="http://stephencoyle.net/ipad-trackpad/">Stephen Coyle came up with a different idea</a>:</p>\n\n<blockquote>\n  <p>The keys on the Smart Keyboard are very low profile, so it’s easy\nfor one’s fingers to glide over them. With this in mind, why not\nmake the entire top surface of the keyboard touch sensitive, then\nuse it in the same way as the software keyboard? All that’s needed\nis a way to toggle trackpad mode, and I think this is a perfect\nopportunity to ditch the &#8220;caps lock&#8221; key, and replace it with a\n&#8220;trackpad mode&#8221; key, which can be held down while using one’s\nother finger to move the cursor.</p>\n\n<p>There are a few reasons why I think this approach would be better\nthan a discrete trackpad. First, it requires no extra space, nor\nany major changes to the current design. Second, as mentioned\nabove, it maintains gesture parity with the current trackpad mode\non iOS. Third, it removes the expectation of a system-wide,\nmouse-style pointer, which I think a laptop-style trackpad would\ncreate. I think this is a significant consideration; a more\nprecise pointing device would be really useful on iOS for more\nthan just text entry, but I don’t expect this to come in the form\nof a mouse pointer. Thus, I think avoiding the suggestion of one\naltogether would lead to less confusion. With my proposed method,\npro users who need this functionality won’t take long to become\naware of it, and users who don’t need it won’t have what they may\nperceive as a half-broken laptop trackpad present at all times.</p>\n</blockquote>\n\n<p>Coyle very nicely summarizes the most common objection I&#8217;ve heard to the idea of adding a trackpad to iPad keyboards: that adding a mouse pointer to iOS is a bad idea because when users see the trackpad they’ll expect a mouse cursor system-wide, not just while editing text.</p>\n\n<p>Maybe!</p>\n\n<p>But my gut tells me this concern is overblown. Even if users expect a mouse cursor when they first see the trackpad, they&#8217;ll adjust quickly when they realize it isn&#8217;t there. If they wanted a MacBook they&#8217;d be using a MacBook. By definition anyone using an iPad is not expecting it to act like a Mac. I do think <em>something</em> should happen when you move your finger(s) around the trackpad even when you&#8217;re not editing text, but tvOS shows that that <em>something</em> doesn&#8217;t need to be a mouse cursor moving around the screen.</p>\n\n<p>I have three objections to Coyle&#8217;s alternative solution. First, I think it would prove to be a technical challenge to create a touch-sensitive surface with Apple-quality latency and responsiveness that also doubles as a good surface for typing. The existing Smart Keyboard is rubbery for several reasons, but I don&#8217;t think rubbery can be used for a touchpad, and I think the gaps between keys would result in stuttery cursor movement. Second, making it a two-handed operation is clunky (and it would be two-handed for people who want to move the insertion point using their right hand). Third, making it a mode &#8212; even an easily toggle-able mode &#8212; feels like a bad idea. Modes aren&#8217;t always bad in UI design &#8212; the iOS software keyboard&#8217;s &#8220;move the cursor&#8221; feature is a mode &#8212; but it&#8217;s almost always better to avoid modality if you can. With a discrete trackpad, the keyboard is always the keyboard and the trackpad is always the trackpad. That&#8217;s better than a keyboard that is usually a keyboard but sometimes a trackpad.</p>\n\n<p>I say don&#8217;t overthink it. Just put a small trackpad under the keyboard for moving the insertion point around and Apple TV-style navigation. Simple.</p>\n\n\n\n    ',
    },
    {
      title: 'The Incomparable: ‘The Godfather Part II’',
      date_published: '2017-05-31T14:36:33Z',
      date_modified: '2017-05-31T14:36:35Z',
      id:
        'https://daringfireball.net/linked/2017/05/31/the-incomparable-the-godfather-part-ii',
      url:
        'https://daringfireball.net/linked/2017/05/31/the-incomparable-the-godfather-part-ii',
      external_url: 'https://www.theincomparable.com/theincomparable/355/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Jason Snell invited John Siracusa, Moisés Chiullan, Merlin Mann, and yours truly to The Incomparable for an in-depth discussion of <em>The Godfather Part II</em> &#8212; a movie that&#8217;s both one of my all-time favorites <em>and</em> one of the best movies ever made. I had a blast watching it for the umpteenth time and then discussing it with these gentlemen.</p>\n\n<div>\n<a  title="Permanent link to ‘The Incomparable: &#8216;The Godfather Part II&#8217;’"  href="https://daringfireball.net/linked/2017/05/31/the-incomparable-the-godfather-part-ii">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Uber Fires Anthony Levandowski',
      date_published: '2017-05-31T02:15:12Z',
      date_modified: '2017-05-31T03:05:16Z',
      id:
        'https://daringfireball.net/linked/2017/05/30/uber-anthony-levandowski',
      url:
        'https://daringfireball.net/linked/2017/05/30/uber-anthony-levandowski',
      external_url:
        'https://www.nytimes.com/2017/05/30/technology/uber-anthony-levandowski.html',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Mike Isaac and Daisuke Wakabayashi, reporting for The New York Times:</p>\n\n<blockquote>\n  <p>Uber said Tuesday that it had fired Anthony Levandowski, a star\nengineer brought in to lead the company’s self-driving automobile\nefforts who was accused of stealing trade secrets when he left a\njob at Google. [&#8230;]</p>\n\n<p>That was certainly the case for Mr. Levandowski. Last August, when\nUber announced it had bought Otto, Mr. Kalanick described Mr.\nLevandowski as “one of the world’s leading autonomous engineers,”\na prolific entrepreneur with “a real sense of urgency.”</p>\n</blockquote>\n\n<p>&#8220;A real sense of urgency&#8221; is one way to put it.</p>\n\n<div>\n<a  title="Permanent link to ‘Uber Fires Anthony Levandowski’"  href="https://daringfireball.net/linked/2017/05/30/uber-anthony-levandowski">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'The New Glif Is Out',
      date_published: '2017-05-30T20:13:36Z',
      date_modified: '2017-06-01T02:25:44Z',
      id: 'https://daringfireball.net/linked/2017/05/30/new-glif',
      url: 'https://daringfireball.net/linked/2017/05/30/new-glif',
      external_url: 'https://www.studioneat.com/products/glif',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Speaking of the iPhone-as-a-camera, Studio Neat&#8217;s all-new Glif tripod mount is out. I just got one as a backer of their Kickstarter campaign, and it&#8217;s every bit as good as I&#8217;d hoped. It works with any size phone, in both portrait and landscape, and has additional mounts for things like microphones and hand grips.</p>\n\n<p>Lovely little intro video, too, narrated by Adam Lisagor.</p>\n\n<p><strong>Update:</strong> Fixed the link, sorry about that.</p>\n\n<div>\n<a  title="Permanent link to ‘The New Glif Is Out’"  href="https://daringfireball.net/linked/2017/05/30/new-glif">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '★ Halide',
      date_published: '2017-05-30T19:37:54Z',
      date_modified: '2017-05-30T20:43:17Z',
      id: 'https://daringfireball.net/2017/05/halide',
      url: 'https://daringfireball.net/2017/05/halide',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>It takes some real grit to create a new iPhone camera app in 2017. To say that &#8220;camera apps&#8221; constitute a crowded category in the App Store is an understatement, <em>and</em> every camera app is competing against the system&#8217;s built-in Camera app, which (a) is very good, and (b) has built-in advantages &#8212; like being accessible from the lock screen by swiping right &#8212; that third-party apps can&#8217;t match.</p>\n\n<p><a href="http://halide.cam/">Halide</a> &#8212; a brand-new iPhone camera app designed by <a href="http://dewith.com/">Sebastiaan de With</a> and developed by <a href="https://sandofsky.com">Ben Sandofsky</a> &#8212; rises to the challenge.</p>\n\n<p>The first question Halide needs to answer is why should anyone even consider it instead of the system&#8217;s Camera app. The answer is that the Camera app has a wide range of responsibilities: separate modes for time-lapse, slow-motion video, regular video, regular photos, square photos, and panoramic photos. Camera also has to be usable by <em>all</em> iPhone users. Camera has a few semi-advanced features for still photography, like auto-exposure and auto-focus lock, but for the most part it only functions as a point-and-shoot camera, because it needs to be understood by the most casual of casual users.</p>\n\n<p>Halide only does still photography. No video, no panoramic photos, no time-lapse. That frees precious on-screen real estate for advanced still photography features &#8212; the sort of things photographers expect from &#8220;real&#8221; cameras:</p>\n\n<ul>\n<li>A fast live-updating histogram.</li>\n<li>An easily-toggled, easily-used manual focus mode.</li>\n<li>Manual ISO and white balance controls.</li>\n<li>RAW support.</li>\n<li>Perhaps best of all, at least in my opinion, Halide has a focus peak feature: this highlights, in real time, which parts of the image have the sharpest contrast.</li>\n</ul>\n\n<p>So if you&#8217;re a camera enthusiast, Halide easily passes the bar for features that justify the price (a mere $2.99 for now, but going up to $4.99 in a week). But as I mentioned before, there are dozens, maybe hundreds, of camera apps in the App Store, and none of Halide&#8217;s features are unique to it.</p>\n\n<p>What sets Halide apart is design.</p>\n\n<p><em>How</em> the features are arranged. <em>How</em> they are accessed. <em>How</em> they are indicated visually. With traditional camera hardware, good reviews spend a lot of words talking about not just what the camera does, but what it is like to use. Camera reviewers often obsess over the placement and feel of all the buttons and dials. Halide brings that sort of obsessive attention to the placement and <em>feel</em> of its controls. This sort of maniacal attention to the smallest of details deserves to be celebrated.</p>\n\n<p><a href="https://blog.halide.cam/introducing-halide-1d0868f5175c">Here&#8217;s how de With and Sandofksy put it in their announcement</a>:</p>\n\n<blockquote>\n  <p>Smartphone cameras have improved massively over the years, but the\nshooting experience hasn’t. The built-in camera app is a little\ntoo simple, while advanced apps feel like an airplane cockpit. We\nneeded an elegant app for deliberate and thoughtful photography,\nso we built Halide. [&#8230;]</p>\n\n<p>Lots of little details are scattered around the app, like a full\nfledged retro styled user manual and a custom typeface for the UI\ncalled Halide Router, developed by <a href="http://www.typehigh.nl/">Jelmar Geertsma</a>.</p>\n\n<p>This was a labor of love. It’s an effort by two friends who love\nphotography and found there wasn’t a tool out there that met our\nneeds. We wanted a premium camera for our phone.</p>\n</blockquote>\n\n<p>I love the page-turning in the user manual, and I love the textured shutter button. There&#8217;s a way to do modern UI design while keeping some whimsy, texture, and depth &#8212; and Halide is a wonderful example of how to do it right.</p>\n\n<p>The custom Halide Router typeface is what puts the whole thing over the top in my book. For chrissake just look at it:</p>\n\n<p><a href="/misc/2017/05/halide-router.png" class="noborder">\n  <img\n    src="/misc/2017/05/halide-router.png"\n    alt="Example specimen of the Halide Router typeface."\n    width="425"\n  /></a></p>\n\n<p>Clearly inspired by the lens inscriptions on kit from Leica and Zeiss, it just exudes <em>camera</em>-ness. The fact that Sandofsky and de With went so far as to commission a custom typeface is probably all you need to hear about Halide. Some of you will hear that and think &#8220;<em>That&#8217;s insane, why would anyone waste so much time and effort on a custom typeface just for a few UI controls?</em>&#8221;</p>\n\n<p>The rest of you are like me, and will think, &#8220;<em>That&#8217;s insane, I need to check this out immediately.</em>&#8221;</p>\n\n\n\n    ',
    },
    {
      title: 'Apple to Provide Podcasting Studio On-Site at WWDC',
      date_published: '2017-05-30T18:21:46Z',
      date_modified: '2017-05-31T04:19:39Z',
      id: 'https://daringfireball.net/linked/2017/05/30/wwdc-podcast-studio',
      url: 'https://daringfireball.net/linked/2017/05/30/wwdc-podcast-studio',
      external_url: 'https://9to5mac.com/2017/05/30/wwdc-2017-podcast-studio/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Benjamin Mayo, writing for 9to5Mac:</p>\n\n<blockquote>\n  <p>The schedule for WWDC 2017 has just been announced and includes a\nnew &#8220;Podcast Studio&#8221;. From Tuesday, podcasters will be able to\nreserve one hour slots and record in a specially-made recording\nstudio inside the McEnery Convention Center.</p>\n\n<p>Apple says that the &#8220;fully outfitted studio&#8221; allows for the\ncreation of audio podcasts with up to four guests per show. Apple\nexperts are on hand to provide support and podcasters are given a\ncopy of their session to distribute freely how they see fit.</p>\n</blockquote>\n\n<p>This is a great idea. I&#8217;d be all over this if I weren&#8217;t having my show in front of a live audience &#8212; recording podcasts while traveling is hard, even if you just consider the equipment you need to pack. Not sure the 60-minute limit would work for me, though.</p>\n\n<p>Also, speaking of my live show, the first round of tickets <del>should be available later today</del> will go on sale tomorrow at 12n ET / 9a PT.</p>\n\n<div>\n<a  title="Permanent link to ‘Apple to Provide Podcasting Studio On-Site at WWDC’"  href="https://daringfireball.net/linked/2017/05/30/wwdc-podcast-studio">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Cast Adds Experimental JSON Feed Support',
      date_published: '2017-05-30T18:00:15Z',
      date_modified: '2017-05-30T18:00:17Z',
      id: 'https://daringfireball.net/linked/2017/05/30/cast-json-feed',
      url: 'https://daringfireball.net/linked/2017/05/30/cast-json-feed',
      external_url:
        'https://blog.tryca.st/cast-update-experimental-json-feed-support-e983a14171ac',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Julian Lepinski, creator of Cast:</p>\n\n<blockquote>\n  <p>So I sunk my teeth in, and in about half a day I’d added\nexperimental JSON Feed support to podcasts published with Cast.</p>\n\n<p>Half a day. If you’re wondering whether JSON Feed publishing is\nstraightforward to implement, that’s your answer right there.</p>\n</blockquote>\n\n<p>That half a day includes <a href="http://help.tryca.st/cast-jsonfeed-support/">some custom extensions</a> specific to podcasting (iTunes-style categories and sub-categories, and an &#8220;explicit&#8221; flag).</p>\n\n<blockquote>\n  <p>All the regular experimental caveats apply &#8212; JSON Feed support is\nexperimental, and could change (or disappear) at some point in\nfuture. We’re not yet seeing widespread client support for JSON\nFeed, but someone has to be the first in the pool to get this\nparty started, and I’m happy for that to be Cast.</p>\n</blockquote>\n\n<p>Bootstrapping something new like JSON Feed often feels like it requires magic. Clients tend not to support a new format until publishers are generating it, and publishers tend not to support a new format until client software supports it. That&#8217;s why it matters that JSON Feed is so easy and fun to support. Being easy and fun is a path around the bootstrapping problem.</p>\n\n<p>If you haven&#8217;t heard of Cast before, <a href="https://tryca.st/">it&#8217;s a rather remarkable all-in-one web-based creative platform for recording, editing, and publishing podcasts</a>. You literally don&#8217;t need anything other than Cast to record a podcast (with multiple guests, none of whom need anything other than a web browser and microphone), edit it, and publish it.</p>\n\n<div>\n<a  title="Permanent link to ‘Cast Adds Experimental JSON Feed Support’"  href="https://daringfireball.net/linked/2017/05/30/cast-json-feed">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '[Sponsor] Jamf Now',
      date_published: '2017-05-30T16:30:23-04:00',
      date_modified: '2017-05-30T16:30:24-04:00',
      id: 'https://daringfireball.net/feeds/sponsors/2017/05/jamf_now_1',
      url: 'https://daringfireball.net/feeds/sponsors/2017/05/jamf_now_1',
      external_url:
        'https://www.jamf.com/lp/set-up-manage-and-protect-apple-devices-at-work/?utm_source=daringfireball&utm_medium=text&utm_campaign=2017-22',
      author: {
        name: 'Daring Fireball Department of Commerce',
      },
      content_html:
        '\n<p>For many people, IT is a task and not a career. Now you can support your users without help from IT.</p>\n\n<p>Jamf Now is a simple, cloud-based solution designed to help anyone set up, manage, and protect Apple devices at work. Easily configure company email and Wi-Fi networks, distribute apps to your team, and protect sensitive data without locking down devices.</p>\n\n<p><a href="https://www.jamf.com/lp/set-up-manage-and-protect-apple-devices-at-work/?utm_source=daringfireball&amp;utm_medium=text&amp;utm_campaign=2017-22">Daring Fireball readers can create an account and manage three devices for free</a>. Forever. Each additional device is just $2 per month. <a href="https://signup.jamfcloud.com/?utm_source=daringfireball&amp;utm_medium=text&amp;utm_campaign=2017-22">Create your free account today</a>.</p>\n\n<div>\n<a  title="Permanent link to ‘Jamf Now’"  href="https://daringfireball.net/feeds/sponsors/2017/05/jamf_now_1">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Cooperstown Celebrates 25th Anniversary of ‘Homer at the Bat’',
      date_published: '2017-05-29T22:13:05Z',
      date_modified: '2017-05-31T05:18:03Z',
      id: 'https://daringfireball.net/linked/2017/05/29/homer-at-the-bat',
      url: 'https://daringfireball.net/linked/2017/05/29/homer-at-the-bat',
      external_url:
        'http://m.mlb.com/news/article/232755100/simpsons-episode-honored-by-hall-of-fame/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Joe Posnanski, in his column for MLB.com:</p>\n\n<blockquote>\n  <p>&#8220;It is with great humility I enter the Hall of Fame,&#8221; Simpson said\nin his recorded acceptance speech. &#8220;And it&#8217;s about damn time. I&#8217;m\nfatter than Babe Ruth, balder than Ty Cobb and have one more\nfinger than Mordecai &#8216;Three Finger&#8217; Brown.&#8221;</p>\n\n<p>The thing about &#8220;Homer at the Bat&#8221; that endures is the obvious\nlove for baseball that fills the episode. Yes, of course, there\nare classic Simpsons bits in it, such as Boggs and Barney having a\nviolent barroom argument over the greatest British Prime Minister\n(Lord Palmerston! Pitt the Elder!), Jose Canseco continuously\nrunning into a burning home to save a woman&#8217;s furniture, Roger\nClemens clucking like a chicken, Bart and Lisa arguing about who\ngets to bring Homer a beer after he crushes a game-winning homer\n(&#8220;Kids, kids, you can BOTH bring me a beer&#8221;).</p>\n</blockquote>\n\n<p>&#8220;<a href="https://www.youtube.com/watch?v=gjHOtxCRhnw">Mattingly, get rid of those sideburns</a>!&#8221;</p>\n\n<p>&#8220;What sideburns?&#8221;</p>\n\n<p>&#8220;You heard me, hippie.&#8221;</p>\n\n<div>\n<a  title="Permanent link to ‘Cooperstown Celebrates 25th Anniversary of &#8216;Homer at the Bat&#8217;’"  href="https://daringfireball.net/linked/2017/05/29/homer-at-the-bat">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Castro 2.4 With Enhanced Audio',
      date_published: '2017-05-29T21:45:33Z',
      date_modified: '2017-05-29T21:45:35Z',
      id: 'https://daringfireball.net/linked/2017/05/29/castro-2-4',
      url: 'https://daringfireball.net/linked/2017/05/29/castro-2-4',
      external_url: 'http://blog.supertop.co/post/161095313797/enhanced-audio',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Speaking of Castro, the latest version adds a much-requested feature:</p>\n\n<blockquote>\n  <p>Enhanced Audio improves the listening experience for many podcasts\nand makes it easier to hear in loud environments. Under the hood,\nEnhanced Audio applies a dynamic compressor and a peak limiter to\nincrease volume just where it’s needed.</p>\n\n<p>Enhanced Audio helps when playing a show where voices are at\ndifferent levels and makes it much easier to listen to podcasts in\na car, on public transit, or in a busy noisy place.</p>\n</blockquote>\n\n<p>These smart speed and equalizer features are becoming table stakes for a podcast player today.</p>\n\n<div>\n<a  title="Permanent link to ‘Castro 2.4 With Enhanced Audio’"  href="https://daringfireball.net/linked/2017/05/29/castro-2-4">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Breaker Adds Support for JSON Feed',
      date_published: '2017-05-29T21:41:38Z',
      date_modified: '2017-05-29T21:41:39Z',
      id: 'https://daringfireball.net/linked/2017/05/29/breaker-json-feed',
      url: 'https://daringfireball.net/linked/2017/05/29/breaker-json-feed',
      external_url:
        'https://medium.com/breakeraudio/breaker-adds-support-for-json-feed-eddbd4afd0f3',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Erik Michaels-Ober:</p>\n\n<blockquote>\n  <p>The decentralized structure of podcasts creates a\nchicken-and-egg problem for JSON Feed to gain adoption. There’s\nno incentive for podcasters to publish in JSON Feed as long as\npodcast players don’t support it. And there’s no incentive for\npodcast players to support JSON Feed as long as podcasters don’t\npublish in that format.</p>\n\n<p>Breaker is hoping to break that stalemate by adding support for\nJSON Feed in our latest release. As far as we know, Breaker is the\nfirst podcast player to do so. Unlike <a href="https://medium.com/@breakeraudio/hello-breaker-50e856d7f33c">other features</a> that\ndifferentiate Breaker, we encourage our competitors to follow our\nlead in this area. The sooner all podcast players support JSON\nFeed, the better positioned the entire podcast ecosystem will be\nfor the decades to come.</p>\n</blockquote>\n\n<p>Three years ago <a href="https://daringfireball.net/linked/2014/07/21/podcast-players">I wrote that podcast players had replaced Twitter clients</a> as the leading UI playground &#8212; the space where there&#8217;s a lot of competition and new ideas being tried out. I still think that&#8217;s true. <a href="https://overcast.fm/">Overcast</a> and <a href="http://castro.fm/">Castro</a> keep getting better, and Breaker is a new and interesting take. The big difference with Breaker is that they have a social networking model, where you can follow your fellow Breaker-using friends and get podcast recommendations based on what they&#8217;re listening to.</p>\n\n<div>\n<a  title="Permanent link to ‘Breaker Adds Support for JSON Feed’"  href="https://daringfireball.net/linked/2017/05/29/breaker-json-feed">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title:
        'Russian Hackers Are Using Google’s Own Infrastructure to Hack Gmail Users',
      date_published: '2017-05-29T21:33:23Z',
      date_modified: '2017-05-29T21:33:25Z',
      id: 'https://daringfireball.net/linked/2017/05/29/russian-amp-phishing',
      url: 'https://daringfireball.net/linked/2017/05/29/russian-amp-phishing',
      external_url:
        'https://motherboard.vice.com/en_us/article/russian-hackers-are-using-googles-own-infrastructure-to-hack-gmail-users',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Lorenzo Franceschi-Bicchierai, reporting for Motherboard:</p>\n\n<blockquote>\n  <p>The &#8220;Change Password&#8221; button linked to a short URL from the\nTiny.cc link shortener service, a Bitly competitor. But the\nhackers cleverly disguised it as a legitimate link by using\nGoogle&#8217;s Accelerated Mobile Pages, or AMP. This is a service\nhosted by the internet giant that was originally designed to speed\nup web pages on mobile, especially for publishers. In practice, it\nworks by creating a copy of a website&#8217;s page on Google&#8217;s servers,\nbut it also acts as an open redirect.</p>\n\n<p>According to Citizen Lab researchers, the hackers used Google\nAMP to trick the targets into thinking the email really came\nfrom Google.</p>\n\n<p>&#8220;It&#8217;s a percentage game, you may not get every person you phish\nbut you&#8217;ll get a percentage,&#8221; John Scott-Railton, a senior\nresearcher at Citizen Lab, told Motherboard.</p>\n\n<p>So if the victim had quickly hovered over the button to inspect\nthe link, they would have seen a URL that starts with\ngoogle.com/amp, which seems safe, and it&#8217;s followed by a Tiny.cc\nURL, which the user might not have noticed. (For example:\nhttps://www.google[.]com/amp/tiny.cc/63q6iy)</p>\n</blockquote>\n\n<p>A huge reason that phishing works is that most people just aren&#8217;t technically savvy enough to tell a phony-looking URL from a legitimate one. But a URL that really is coming from the google.com domain &#8212; that&#8217;s the sort of link that even a web developer might think looks legit, especially at a glance.</p>\n\n<div>\n<a  title="Permanent link to ‘Russian Hackers Are Using Google&#8217;s Own Infrastructure to Hack Gmail Users’"  href="https://daringfireball.net/linked/2017/05/29/russian-amp-phishing">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Flow: Simple Project Management',
      date_published: '2017-05-28T00:37:21Z',
      date_modified: '2017-05-28T00:39:37Z',
      id: 'https://daringfireball.net/linked/2017/05/27/flow',
      url: 'https://daringfireball.net/linked/2017/05/27/flow',
      external_url: 'http://www.getflow.com/daringfireball',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>My thanks to Flow for sponsoring this week&#8217;s DF RSS feed. Flow is simple project management for busy teams. It’s the easiest way to run your team, manage projects, track tasks, and stay up to date with everything happening at work.</p>\n\n<p>Teams choose Flow when email, sticky notes, and to-do apps aren’t enough, but complex project management tools are overkill. Flow’s world-class design team has worked with companies like Apple, Slack, TED, and Starbucks. It’s simple, beautiful, and easy to use. Your team will love using it. Team-based &#8220;project management&#8221; is a <em>really</em> tricky problem, and Flow has solved it in a simple and elegant way. And of course they have <a href="https://www.getflow.com/apps">great apps for iPhone, Mac, Android, and Windows</a>.</p>\n\n<p>Special offer for DF readers: <a href="http://www.getflow.com/daringfireball">Start your free trial today</a>, and save 20 percent on a monthly plan or 30 percent on an annual plan at checkout. </p>\n\n<div>\n<a  title="Permanent link to ‘Flow: Simple Project Management’"  href="https://daringfireball.net/linked/2017/05/27/flow">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'The Talk Show: ‘He Ends Up Fighting Hervé Villechaize’',
      date_published: '2017-05-28T00:14:52Z',
      date_modified: '2017-05-28T00:14:55Z',
      id: 'https://daringfireball.net/linked/2017/05/27/the-talk-show-191',
      url: 'https://daringfireball.net/linked/2017/05/27/the-talk-show-191',
      external_url: 'https://daringfireball.net/thetalkshow/2017/05/27/ep-191',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>New episode of America&#8217;s favorite 3-star podcast, with special guest Jim Dalrymple. We speculate about what Apple might announce at WWDC 2017: Apple Watch, iPad, iOS, updated MacBooks, Apple TV, and more. Also: a celebration of the great Roger Moore.</p>\n\n<p>Brought to you by these great sponsors:</p>\n\n<ul>\n<li><a href="http://mailroute.net/tts">MailRoute</a>: Hosted spam and virus protection for email. Use this link for 10 percent off for the life of your account.</li>\n<li><a href="https://squarespace.com/talkshow">Squarespace</a>: Make your next move with a beautiful website. Use code <strong>gruber</strong> for 10 percent off your first order.</li>\n<li><a href="https://www.fractureme.com/podcast">Fracture</a>: Your photos printed in vivid color directly on glass. Great gift idea.</li>\n</ul>\n\n<div>\n<a  title="Permanent link to ‘The Talk Show: ‘He Ends Up Fighting Hervé Villechaize’’"  href="https://daringfireball.net/linked/2017/05/27/the-talk-show-191">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title:
        'Washington Post: ‘Google Now Knows When Its Users Go to the Store and Buy Stuff’',
      date_published: '2017-05-27T00:38:22Z',
      date_modified: '2017-05-28T00:49:15Z',
      id: 'https://daringfireball.net/linked/2017/05/26/google-buy-stuff',
      url: 'https://daringfireball.net/linked/2017/05/26/google-buy-stuff',
      external_url:
        'https://www.washingtonpost.com/news/the-switch/wp/2017/05/23/google-now-knows-when-you-are-at-a-cash-register-and-how-much-you-are-spending/',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Elizabeth Dwoskin and Craig Timberg, writing for The Washington Post:</p>\n\n<blockquote>\n  <p>Google has begun using billions of credit-card transaction records\nto prove that its online ads are prompting people to make\npurchases &#8212; even when they happen offline in brick-and-mortar\nstores, the company said Tuesday.</p>\n\n<p>The advance allows Google to determine how many sales have been\ngenerated by digital ad campaigns, a goal that industry insiders\nhave long described as “the holy grail” of online advertising. But\nthe announcement also renewed long-standing privacy complaints\nabout how the company uses personal information.</p>\n</blockquote>\n\n<p><a href="https://adwords.googleblog.com/2017/05/powering-ads-and-analytics-innovations.html">Here&#8217;s Google&#8217;s announcement about this</a>. I can&#8217;t figure out <em>how</em> it works. But it sounds creepy as hell. This is why I don&#8217;t grant Google any background access to my location data.</p>\n\n<div>\n<a  title="Permanent link to ‘Washington Post: &#8216;Google Now Knows When Its Users Go to the Store and Buy Stuff&#8217;’"  href="https://daringfireball.net/linked/2017/05/26/google-buy-stuff">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Follow-Up on Edition Numbering and the Marc Newson Hourglass',
      date_published: '2017-05-26T22:34:28Z',
      date_modified: '2017-05-26T23:32:41Z',
      id: 'https://daringfireball.net/linked/2017/05/26/newson-edition-number',
      url: 'https://daringfireball.net/linked/2017/05/26/newson-edition-number',
      external_url:
        'https://www.hodinkee.com/articles/marc-newson-hourglass-limited-edition-for-hodinkee',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Small point of follow-up regarding <a href="https://daringfireball.net/linked/2017/05/24/marc-newson-hourglass">my post</a> the other day about Hodinkee&#8217;s $12,000 hourglass designed by Marc Newson. I wrote:</p>\n\n<blockquote>\n  <p>I do find it odd that every unit is numbered “1/100” rather than\ngiving each piece a unique number.</p>\n</blockquote>\n\n<p>I later clarified that to:</p>\n\n<blockquote>\n  <p>I do find it odd that every unit is numbered “1/100” rather than\ngiving each piece a unique number &#8212; “1/100”, “2/100”, … “100/100”.</p>\n</blockquote>\n\n<p>But I keep getting email about this. I am aware that this is how <a href="http://www.artcellarexchange.com/glossary_printmaking.html">edition numbering works</a>:</p>\n\n<blockquote>\n  <p><em>Edition Number:</em> A fraction found on the bottom left hand corner of a print. The top number is the sequence in the edition; the bottom number is the total number of prints in the edition. The number appears as a fraction usually in the lower left of the print. For instance the edition number 25/50 means that it is print number 25 out of a total edition of 50.</p>\n</blockquote>\n\n<p>That&#8217;s exactly what I think Hodinkee should be doing with these hourglasses, but <a href="https://www.hodinkee.com/articles/marc-newson-hourglass-limited-edition-for-hodinkee">from their own description</a>, they&#8217;re not:</p>\n\n<blockquote>\n  <p>The Marc Newson Hourglass for Hodinkee is a limited edition of 100\npieces. Each is numbered &#8220;1 of 100&#8221; just below the &#8220;Hodinkee&#8221;\nsignature on one side, with Marc Newson&#8217;s signature on the\nopposite side.</p>\n</blockquote>\n\n<p>That says to me that all 100 pieces are numbered &#8220;1 of 100&#8221;. My guess is that the nature of the glass makes it difficult to print a unique number on each piece, but for $12,000 I would expect no expense to be spared. Also, when you label each piece with a unique number, owners of the pieces can feel more confident that theirs is unique. E.g. if it were ever discovered that two of them were labeled &#8220;12/100&#8221;, you would know something fishy is going on. I don&#8217;t think Hodinkee is secretly selling more than 100 of these, I&#8217;m just pointing out why it would be nicer if they were sequentially numbered.</p>\n\n<div>\n<a  title="Permanent link to ‘Follow-Up on Edition Numbering and the Marc Newson Hourglass’"  href="https://daringfireball.net/linked/2017/05/26/newson-edition-number">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Yoink',
      date_published: '2017-05-26T19:44:59Z',
      date_modified: '2017-05-26T23:33:44Z',
      id: 'https://daringfireball.net/linked/2017/05/26/yoink',
      url: 'https://daringfireball.net/linked/2017/05/26/yoink',
      external_url:
        'http://eternalstorms.at/yoink/Yoink_-_Simplify_and_Improve_Drag_and_Drop_on_your_Mac/Yoink_-_Simplify_drag_and_drop_on_your_Mac.html',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Yoink is a terrific utility for MacOS by Matthias Gansrigler. It gives you a shelf at the side of your screen where you can drop files (or clippings, like URLs or text snippets). Think of it as a place to park drag-and-drop items temporarily, while you switch apps or whatever. Cheap too: just $7 (<a href="https://itunes.apple.com/app/yoink/id457622435?mt=12">here it is in the Mac App Store</a>). Be sure to check out <a href="http://eternalstorms.at/yoink/Yoink_-_Simplify_and_Improve_Drag_and_Drop_on_your_Mac/Yoink_-_Usage_Tips.html">the usage tips</a> &#8212; I&#8217;ve been using Yoink for over six months, and I learned a few things just now.</p>\n\n<p>Back in 2012 <a href="https://daringfireball.net/linked/2012/05/08/dragondrop">I recommended a similar utility</a> called DragonDrop, but <a href="https://twitter.com/dragondropapp/status/618642103996821505?lang=en">DragonDrop is on hiatus</a>, and I think I much prefer Yoink&#8217;s interface.</p>\n\n<div>\n<a  title="Permanent link to ‘Yoink’"  href="https://daringfireball.net/linked/2017/05/26/yoink">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Mossberg: The Disappearing Computer',
      date_published: '2017-05-26T03:56:37Z',
      date_modified: '2017-05-26T03:56:39Z',
      id: 'https://daringfireball.net/linked/2017/05/25/mossberg',
      url: 'https://daringfireball.net/linked/2017/05/25/mossberg',
      external_url:
        'https://www.recode.net/2017/5/25/15689094/mossberg-final-column',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Walt Mossberg:</p>\n\n<blockquote>\n  <p>This is my last weekly column for The Verge and Recode &#8212; the last\nweekly column I plan to write anywhere. I’ve been doing these\nalmost every week since 1991, starting at the Wall Street Journal,\nand during that time, I’ve been fortunate enough to get to know\nthe makers of the tech revolution, and to ruminate &#8212; and\nsometimes to fulminate &#8212; about their creations.</p>\n\n<p>Now, as I prepare to retire at the end of that very long and\nworld-changing stretch, it seems appropriate to ponder the\nsweep of consumer technology in that period, and what we can\nexpect next.</p>\n</blockquote>\n\n<p>Godspeed on whatever&#8217;s next, Walt.</p>\n\n<div>\n<a  title="Permanent link to ‘Mossberg: The Disappearing Computer’"  href="https://daringfireball.net/linked/2017/05/25/mossberg">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'Nick Murray on Alcantara: ‘It’s Garbage’',
      date_published: '2017-05-25T17:03:28Z',
      date_modified: '2017-05-26T04:02:32Z',
      id: 'https://daringfireball.net/linked/2017/05/25/murray-alcantara',
      url: 'https://daringfireball.net/linked/2017/05/25/murray-alcantara',
      external_url:
        'https://www.youtube.com/watch?v=aebUNgMhQV4&feature=youtu.be',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Interesting video by Nick Murray, discussing the merits of <a href="http://www.alcantara.com/">Alcantara</a>, the synthetic suede-like product that <a href="https://www.microsoft.com/en-us/surface/devices/surface-laptop/innovation">Microsoft has used</a> for their new Surface Laptop. Murray is coming from the perspective of Alcantara&#8217;s use in cars, not laptops, but he says it wears terribly on things you touch, like steering wheels and gear shifters, losing all its softness after just a few thousand miles. This might bode poorly for the Surface Laptop.</p>\n\n<div>\n<a  title="Permanent link to ‘Nick Murray on Alcantara: &#8216;It&#8217;s Garbage&#8217;’"  href="https://daringfireball.net/linked/2017/05/25/murray-alcantara">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: 'The Marc Newson Hourglass for Hodinkee',
      date_published: '2017-05-25T03:59:00Z',
      date_modified: '2017-05-25T17:08:45Z',
      id: 'https://daringfireball.net/linked/2017/05/24/marc-newson-hourglass',
      url: 'https://daringfireball.net/linked/2017/05/24/marc-newson-hourglass',
      external_url:
        'https://www.hodinkee.com/articles/marc-newson-hourglass-limited-edition-for-hodinkee',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Watch the video and read this. I&#8217;ll update this post with my comments later today.</p>\n\n<p><strong>Update:</strong> OK, so my take on this is going to upset many of you. I first saw this last night via <a href="https://twitter.com/marcoarment/status/867210701697409024">this tweet from Marco Arment</a>, and I read through the replies and every single one of them was mocking either the entire premise of an exquisite hourglass or at the very least the price.</p>\n\n<p>I think this looks beautiful, and I don&#8217;t think there&#8217;s anything crazy about it costing $12,000. I&#8217;m not buying one. But all sorts of pieces of art cost tens of thousands of dollars, and I say this is most definitely art. Newson&#8217;s previous hourglass design, for Ikepod, <a href="http://www.ablogtowatch.com/ikepod-hourglass-time-for-art/">ranged from $13,000–40,000</a>.</p>\n\n<p>I do find it odd that every unit is numbered &#8220;1/100&#8221; rather than giving each piece a unique number &#8212; &#8220;1/100&#8221;, &#8220;2/100&#8221;, … &#8220;100/100&#8221;. And Hodinkee isn&#8217;t doing themselves any favors with some of the precious bits of copywriting (e.g. &#8220;approximately 1,249,996 little spheres&#8221; is not an approximation). But if you don&#8217;t see anything ludicrous about a mechanical watch costing in excess of $10,000 (or $100,000, <a href="https://www.forbes.com/sites/robertanaas/2017/05/13/rolex-bao-dai-watch-sells-for-more-than-5-million-at-phillips-auction-new-world-record/">or more</a>) why is there something ludicrous about a $12,000 hourglass?</p>\n\n<p>The world is full of cheaply-made mass-produced crap. Why not celebrate the creation of something genuinely beautiful?</p>\n\n<div>\n<a  title="Permanent link to ‘The Marc Newson Hourglass for Hodinkee’"  href="https://daringfireball.net/linked/2017/05/24/marc-newson-hourglass">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '‘Spectre’ Trailer Re-Cut With Roger Moore as Bond',
      date_published: '2017-05-24T21:28:28Z',
      date_modified: '2017-05-25T17:49:07Z',
      id: 'https://daringfireball.net/linked/2017/05/24/moore-spectre',
      url: 'https://daringfireball.net/linked/2017/05/24/moore-spectre',
      external_url:
        'http://io9.gizmodo.com/the-spectre-trailer-is-so-much-better-when-its-starring-1723648560',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>This is so well done it gave me goosebumps. Makes me think the franchise could use some Moore-like suaveness when they recast the role post-Craig.</p>\n\n<div>\n<a  title="Permanent link to ‘&#8216;Spectre&#8217; Trailer Re-Cut With Roger Moore as Bond’"  href="https://daringfireball.net/linked/2017/05/24/moore-spectre">&nbsp;★&nbsp;</a>\n</div>\n\n\t',
    },
    {
      title: '★ Safari vs. Chrome on the Mac',
      date_published: '2017-05-24T20:36:39Z',
      date_modified: '2017-05-25T17:14:06Z',
      id: 'https://daringfireball.net/2017/05/safari_vs_chrome_on_the_mac',
      url: 'https://daringfireball.net/2017/05/safari_vs_chrome_on_the_mac',
      author: {
        name: 'John Gruber',
      },
      content_html:
        '\n<p>Eric Petitt, <a href="https://medium.com/the-official-unofficial-firefox-blog/browse-against-the-machine-e793c0fee917">writing for The Official Unofficial Firefox Blog yesterday</a>:</p>\n\n<blockquote>\n  <p>I head up Firefox marketing, but I use Chrome every day. Works\nfine. Easy to use. Like most of us who spend too much time in\nfront of a laptop, I have two browsers open; Firefox for work,\nChrome for play, customized settings for each. There are multiple\nthings that bug me about the Chrome product, for sure, but I‘m OK\nwith Chrome. I just don’t like <em>only</em> being on Chrome. [&#8230;]</p>\n\n<p>But talking to friends, it sounds more and more like living on\nChrome has started to feel like their only option. Edge is broken.\nSafari and Internet Explorer are just plain bad. And\nunfortunately, too many people think Firefox isn’t a modern\nalternative.</p>\n</blockquote>\n\n<p>In an update posted today, he walked that back:</p>\n\n<blockquote>\n  <p>In my original post I made a personal dig about Edge, IE and\nSafari: “Edge is broken. Safari and Internet Explorer are just\nplain bad.” I’ve since deleted that sentence.</p>\n\n<p>It’s true, I personally don’t like those products, they just don’t\nwork for me. But that was probably a bit too flip. And, if it\nwasn’t obvious that those were my personal opinions as a user, not\nthose of the good folks at Firefox and Mozilla, then please accept\nmy apology.</p>\n</blockquote>\n\n<p>It&#8217;s easy when making an aside &#8212; and it&#8217;s clear that the central premise of this piece is about positioning Chrome as the Goliath to Firefox&#8217;s David, so references to Safari and IE are clearly asides &#8212; to conflate &#8220;<em>I don&#8217;t like X</em>&#8221; with &#8220;<em>X is bad</em>&#8221;. So I say we let it slide.<sup id="fnr1-2017-05-24"><a href="#fn1-2017-05-24">1</a></sup></p>\n\n<p>But I&#8217;ve been meaning to write about Safari vs. Chrome for a while, and Petitt&#8217;s jab, even retracted, makes for a good excuse.</p>\n\n<p>I think Safari is a <em>terrific</em> browser. It remains the one and only browser for the Mac that behaves like a native Mac app through and through. It may not be the fastest browser but it is fast. And its energy performance puts Chrome to shame. If you use a Mac laptop, using Chrome instead of Safari can cost you an hour or more of battery life per day.<sup id="fnr2-2017-05-24"><a href="#fn2-2017-05-24">2</a></sup></p>\n\n<p>But Chrome is a terrific browser, too. It&#8217;s clearly the second-most-Mac-like browser for MacOS. It almost inarguably has the widest and deepest extension ecosystem. It has good web developer tools, and Chrome adopts new web development technologies faster than Safari does.</p>\n\n<p>But Safari&#8217;s extension model is more privacy-conscious. For many people on MacOS, the decision between Safari and Chrome probably comes down to which ecosystem you&#8217;re more invested in &#8212; iCloud or Google &#8212; for things like tab, bookmark, and history syncing. Me, personally, I&#8217;d feel lost without the ability to send tabs between my Macs and iPhone via Handoff. <strong>Update:</strong> Unbeknownst to me, Chrome fully supports Handoff with iOS devices. Nice!</p>\n\n<p>In short, Safari closely reflects Apple&#8217;s institutional priorities (privacy, energy efficiency, the niceness of the native UI, support for MacOS and iCloud technologies) and Chrome closely reflects Google&#8217;s priorities (speed, convenience, a web-centric rather than native-app-centric concept of desktop computing, integration with Google web properties). Safari is Apple&#8217;s browser for Apple devices. Chrome is Google&#8217;s browser for all devices.</p>\n\n<p>I personally prefer Safari, but I can totally see why others &#8212; especially those who work on desktop machines or MacBooks that are usually plugged into power &#8212; prefer Chrome. DF readers agree. Looking at my web stats, over the last 30 days, 69 percent of Mac users visiting DF used Safari, but a sizable 28 percent used Chrome. (Firefox came in at 3 percent, and everything else was under 1 percent.)<sup id="fnr3-2017-05-24"><a href="#fn3-2017-05-24">3</a></sup></p>\n\n<p>As someone who&#8217;s been a Mac user long enough to remember when there were <em>no</em> good web browsers for the Mac, having both Safari and Chrome feels downright bountiful, and the competition is making both of them better.</p>\n\n<div class="footnotes">\n<hr />\n<ol>\n<li id="fn1-2017-05-24">\n<p>What really struck me about Petitt&#8217;s piece wasn&#8217;t the unfounded (to my eyes) dismissal of Safari, but rather his admission that he uses &#8220;Firefox for work,  Chrome for play&#8221;. I really doubt the marketing managers for Chrome or Safari spend their days with a rival browser open for &#8220;play&#8221;, and even if they did, I expect they&#8217;d have the common sense not to admit so publicly, and especially not in the opening paragraph of a piece arguing that their own browser is a viable alternative to the rival one.&nbsp;<a href="#fnr1-2017-05-24"  class="footnoteBackLink"  title="Jump back to footnote 1 in the text.">&#x21A9;&#xFE0E;</a></p>\n</li>\n<li id="fn2-2017-05-24">\n<p>Back in December, when Consumer Reports rushed out their sensational report <a href="https://daringfireball.net/linked/2016/12/23/cr-mbp">claiming bizarrely erratic battery life</a> on the then-new MacBook Pros (which was eventually determined to be <a href="https://daringfireball.net/linked/2017/01/12/consumer-reports">caused by a bug in Safari that Apple soon fixed</a>), I decided to try to loosely replicate their test on the MacBook Pro review units I had from Apple. Consumer Reports doesn&#8217;t reveal the exact details of their testing, but they do describe it in general. They set the laptop brightness to a certain brightness value, then load a list of web pages repeatedly until the battery runs out. Presumably they automate this with a script of some sort, but they don&#8217;t say.</p>\n\n<p>That&#8217;s pretty easy to replicate in AppleScript. I used that day&#8217;s leading stories on <a href="https://www.techmeme.com/">TechMeme</a> as my source for URLs to load &#8212; 26 URLs total. When a page loads, my script waits 5 seconds, and then scrolls down (simulating the Page Down key), waits another 5 seconds and pages down again, and then waits another 5 seconds before paging down one last time. This is a simple simulation of a person actually reading a web page. While running through the list of URLs, my script leaves each URL open in a tab. At the end of the list, it closes all tabs and then starts all over again. Each time through the loop the elapsed time and remaining battery life are logged to a file. (I also logged results as updates via messages sent to myself via iMessage, so I could monitor the progress of the hours-long test runs from my phone. No apps were running during the tests other than Safari, Script Editor, Finder, and Messages.)</p>\n\n<p>I set the display brightness at exactly 68.75 percent for each test (11/16 clicks on the brightness meter when using the function key buttons to adjust), a value I chose arbitrarily as a reasonable balance for someone running on battery power.</p>\n\n<p>Averaged (and rounded) across several runs, I got the following results:</p>\n\n<ul>\n<li>15-inch MacBook Pro With Touch Bar: 6h:50m</li>\n<li>13-inch MacBook Pro With Touch Bar: 5h:30m</li>\n<li>13-inch MacBook Pro (2014): 5h:10m</li>\n<li>11-inch MacBook Air (2011): 2h:15m</li>\n</ul>\n\n<p>I no longer had a new 13-inch MacBook Pro without the Touch Bar (a.k.a. the &#8220;MacBook Esc&#8221;) &#8212; I&#8217;d sent it back to Apple. I included my own personal 2014 13-inch MacBook Pro and my old 2011 MacBook Air just as points of reference. I think the Air did poorly just because it was so old and so well-used. It still has its original battery.</p>\n\n<p>I saw <em>no</em> erratic fluctuations in battery life across runs of the test. I procrastinated on publishing the results, though, and within a few weeks the whole thing was written off with a &#8220;<em>never mind!</em>&#8221; when Apple fixed the bug in Safari that was causing Consumer Reports&#8217;s erratic results.</p>\n\n<p>Anyway, the whole point of including these results in this footnote is that I also ran the exact same test with Chrome on the 13-inch MacBook Pro With Touch Bar. The average result: 3h:40m. That&#8217;s 1h:50m difference. On the exact same machine running the exact same test with the exact same list of URLs, the battery lasted almost exactly 1.5 times as long using Safari than Chrome.</p>\n\n<p>My test was in no way meant to simulate real-world usage. You&#8217;d have to be fueled up on some serious stimulants to read a new web page every 15 seconds non-stop for hours on end. But the results were striking. If you place a high priority on your MacBook&#8217;s battery life, you should use Safari instead of Chrome.</p>\n\n<p>If you&#8217;re interested, I&#8217;ve posted my battery testing scripts for <a href="https://gist.github.com/gruber/ad201668b31d21096456d7abf11acbd3">Safari</a> and <a href="https://gist.github.com/gruber/15d7183f04c2ac1c51ee6a2637925ebd">Chrome</a>.&nbsp;<a href="#fnr2-2017-05-24"  class="footnoteBackLink"  title="Jump back to footnote 2 in the text.">&#x21A9;&#xFE0E;︎</a></p>\n</li>\n<li id="fn3-2017-05-24">\n<p>If anyone has a good source for browser usage by MacOS users from a general purpose website like The New York Times or CNN, let me know. I honestly don&#8217;t know whether to expect that the split among DF readers is biased in favor of Safari because DF readers are more likely to care about the advantages of a native app, or biased in favor of Chrome because so many of you are web developers or even just nerdy enough to install a third-party browser in the first place. Wikimedia used to publish stats like that, but alas, <a href="https://stats.wikimedia.org/wikimedia/squids/SquidReportClients.htm">ceased in 2015</a>.&nbsp;<a href="#fnr3-2017-05-24"  class="footnoteBackLink"  title="Jump back to footnote 3 in the text.">&#x21A9;&#xFE0E;︎</a></p>\n</li>\n\n</ol>\n</div>\n\n\n\n    ',
    },
  ],
}
