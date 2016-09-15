import TasksPage from 'src/tasks/containers/TasksPage';
import RebalanceModal from 'src/tasks/components/RebalanceModal';
import React from 'react';
import {mount, shallow} from 'enzyme';

const clusterID = '1000';
const JOBS = [
  {
    "source": "localhost:8088",
    "dest": "localhost:8188",
    "id": 20,
    "status": "Planned"
  },
  {
    "source": "localhost:8088",
    "dest": "localhost:8188",
    "id": 10,
    "status": "Planned"
  },
];

function setup(customProps = {}) {
  const props = Object.assign({}, {
    params: {clusterID},
  }, customProps);
  return mount(<TasksPage {...props} />);
}

function setupShallow(customProps = {}) {
  const props = Object.assign({}, {
    params: {clusterID},
  }, customProps);
  return shallow(<TasksPage {...props} />);
}

describe('Tasks.Containers.TasksPage', function() {
  before(function() { this.server = sinon.fakeServer.create(); });
  after(function()  { this.server.restore() });

  beforeEach(function() {
    this.server.respondWith("GET", /\/api\/int\/v1\/clusters\/1000\/authorized/, [
      200, { "Content-Type": "application/json" }, '']);
    this.server.respondWith("GET", '/api/v1/jobs', [
      200, { "Content-Type": "application/json" }, '']);
  });

  describe('in intial state with no tasks', function() {
    it('renders a an empty state for the task list', function() {
      const wrapper = setupShallow();

      const table = wrapper.find('.tasks-empty-state');
      expect(table.text()).to.match(/No tasks/);
    });

    it('renders a legend', function() {
      const wrapper = setupShallow();

      const legend = wrapper.find('.dot-legend');
      expect(legend.text()).to.match(/Running/);
      expect(legend.text()).to.match(/Planned/);
    });

    it('renders a rebalance modal', function() {
      const wrapper = setupShallow();

      expect(wrapper.find(RebalanceModal).length).to.equal(1);
    });

    it('sends a request to check if a user is authorized', function(done) {
      const wrapper = setup();

      this.server.respond();

      setTimeout(() => {
        const request = this.server.requests.find(r => r.url.match(/\/api\/int\/v1\/clusters\/1000\/authorized/))
        expect(request).to.be.ok;
        expect(request.method).to.equal('GET');
        done();
      });
    });

    it('fetches a list of active tasks', function(done) {
      const wrapper = setup();

      this.server.respond();

      setTimeout(() => {
        const request = this.server.requests.find(r => r.url.match(/\/api\/v1\/jobs/));
        expect(request).to.be.ok;
        expect(request.method).to.equal('GET');
        done();
      });
    });
  });

  describe('when there are active tasks', function() {
    it('renders a table row for each task');
  });

  describe('with valid permissions', function() {
    it('renders the rebalance button', function() {
      const wrapper = setupShallow();

      // TODO: get this working with FakeServer. For some reason I can't
      // get it to respond and trigger the `then` block in componentDidMount.
      // Lots of async issues with testing containers like this :(.
      wrapper.setState({canRebalance: true});
      wrapper.update();

      const button = wrapper.find('button.rebalance');

      expect(button.length).to.equal(1);
    });

    it('sends a request to rebalance after the rebalance button is clicked');
  });

  describe('with invalid permissions', function() {
    it('doesn\'t render the rebalance button');
  });
});
