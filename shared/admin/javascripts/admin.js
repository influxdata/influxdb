var QueryField = react.createClass({
  render: function() {
    return (
      <form>
        <div class="form-group">
          <input type="text" class="form-control" id="query">
        </div>
      </form>
    )
  }
});

var DataTable = react.createClass({
  render: function() {
    return (
      <table class="table">
        <th>
          <td>Field</td>
        </th>
        <TableRow data={this.state.data} />
      </table>
    )
  }
};)

React.render(
  <QueryField />,
  document.getElementById('content')
);
