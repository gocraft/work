import expect from 'expect';
import ScheduledJobs from './ScheduledJobs';
import React from 'react';
import ReactTestUtils from 'react-addons-test-utils';

describe('ScheduledJobs', () => {
  it('shows jobs', () => {
    let r = ReactTestUtils.createRenderer();
    r.render(<ScheduledJobs />);
    let processes = r.getMountedInstance();

    expect(processes.state.Jobs.length).toEqual(0);
  });
});
