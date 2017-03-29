import React from 'react'

const NotFound = React.createClass({
  render() {
    return (
      <div className="container-fluid">
        <div className="panel panel-error panel-spring">
          <div className="panel-heading text-center">
            <h1 className="deluxe">404</h1>
            <h4>Bummer! We couldn't find the page you were looking for</h4>
          </div>
        </div>
      </div>
    )
  },
})

export default NotFound
