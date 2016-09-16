# mrfusion

## Builds
To build assets and the go server, run `make`.
To run server either `./mrfusion --port 8888` or `make run`

## Deployment (for now)
Includes a Dockerfile that builds a container suitable for Heroku.

In order to push to heroku, make sure you've logged into Heroku normally with...

`heroku login`

Add the acceptance server git remote...

`git remote add acceptance https://git.heroku.com/mrfusion-acc.git`

When you run `heroku apps` you should see "mrfusion-acc".

Then install the container plugin
`heroku plugins:install heroku-container-registry`

Then log into the container registry with...

`heroku container:login`

Build and push the web container by running...

`heroku container:push web`

