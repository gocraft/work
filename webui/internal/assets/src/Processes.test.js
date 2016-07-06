import expect from 'expect';
import Processes from './Processes';
import React from 'react';
import ReactTestUtils from 'react-addons-test-utils';

describe('Processes', () => {
  it('shows workers', () => {
    let r = ReactTestUtils.createRenderer();
    r.render(<Processes />);
    let processes = r.getMountedInstance();

    expect(processes.state.busyWorker.length).toEqual(0);
    expect(processes.state.workerPool.length).toEqual(0);
  });
});
