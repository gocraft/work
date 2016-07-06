import expect from 'expect';
import RetryJobs from './RetryJobs';
import React from 'react';
import ReactTestUtils from 'react-addons-test-utils';

describe('RetryJobs', () => {
  it('shows jobs', () => {
    let r = ReactTestUtils.createRenderer();
    r.render(<RetryJobs />);
    let processes = r.getMountedInstance();

    expect(processes.state.Jobs.length).toEqual(0);
  });
});
