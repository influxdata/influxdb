# chronograf

## Builds

* To build assets and the go server, run `make`.
* To run server either `./chronograf --port 8888` or `make run`
* If you add files to the javascript build and you don't want to rebuild everything, run `make bindata && make chronograf`

## Deployment (for now)
Includes a Dockerfile that builds a container suitable for Heroku.

In order to push to heroku, make sure you've logged into Heroku normally with...

`heroku login`

Add the acceptance server git remote...

`git remote add acceptance https://git.heroku.com/chronograf-acc.git`

When you run `heroku apps` you should see "chronograf-acc".

Then install the container plugin
`heroku plugins:install heroku-container-registry`

Then log into the container registry with...

`heroku container:login`

Build and push the web container by running...

`heroku container:push web`

