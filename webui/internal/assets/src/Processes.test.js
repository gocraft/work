import expect from 'expect';
import Processes from './Processes';
import React from 'react';
import ReactTestUtils from 'react-addons-test-utils';
import { findAllByTag } from './TestUtils';

describe('Processes', () => {
  it('shows workers', () => {
    let r = ReactTestUtils.createRenderer();
    r.render(<Processes />);
    let processes = r.getMountedInstance();

    expect(processes.state.busyWorker.length).toEqual(0);
    expect(processes.state.workerPool.length).toEqual(0);

    processes.setState({
      busyWorker: [
        {
          WorkerID: '2',
          JobName: 'job1',
          StartedAt: 1467753603,
          CheckinAt: 1467753603,
          Checkin: '123',
          ArgsJSON: {}
        }
      ],
      workerPool: [
        {
          WorkerPoolID: '1',
          StartedAt: 1467753603,
          HeartbeatAt: 1467753603,
          JobNames: ['job1', 'job2', 'job3', 'job4'],
          Concurrency: 10,
          Host: 'web51',
          Pid: 123,
          WorkerIDs: [
            '1', '2', '3'
          ]
        }
      ]
    });

    expect(processes.state.busyWorker.length).toEqual(1);
    expect(processes.state.workerPool.length).toEqual(1);
    expect(processes.workerCount).toEqual(3);

    const expectedBusyWorker = [ { ArgsJSON: {}, Checkin: '123', CheckinAt: 1467753603, JobName: 'job1', StartedAt: 1467753603, WorkerID: '2' } ];

    let output = r.getRenderOutput();
    let busyWorkers = findAllByTag(output, 'BusyWorkers');
    expect(busyWorkers.length).toEqual(1);
    expect(busyWorkers[0].props.worker).toEqual(expectedBusyWorker);
    expect(processes.getBusyPoolWorker(processes.state.workerPool[0])).toEqual(expectedBusyWorker);
  });
});
