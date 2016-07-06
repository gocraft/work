import expect from 'expect';
import DeadJobs from './DeadJobs';
import React from 'react';
import ReactTestUtils from 'react-addons-test-utils';

describe('DeadJobs', () => {
  it('shows dead jobs', () => {
    let r = ReactTestUtils.createRenderer();
    r.render(<DeadJobs />);
    let deadJobs = r.getMountedInstance();

    expect(deadJobs.state.selected.length).toEqual(0);
    expect(deadJobs.state.Jobs.length).toEqual(0);

    deadJobs.setState({
      Count: 2,
      Jobs: [
        {id: 1, name: 'test', args: {}, t: 1467760821, err: 'err1'},
        {id: 2, name: 'test2', args: {}, t: 1467760822, err: 'err2'}
      ]
    });

    expect(deadJobs.state.selected.length).toEqual(0);
    expect(deadJobs.state.Jobs.length).toEqual(2);
  });
});
