const chai = require('chai');
const sinon = require('sinon');
const utils = require('../lib/utils');

const expect = chai.expect;

describe('utils', () => {
  let sandbox;
  let clock;
  beforeEach(() => {
    sandbox = sinon.sandbox.create();
    clock = sinon.useFakeTimers('setTimeout', 'Date');
  });

  afterEach(() => {
    sandbox.restore();
    clock.restore();
  });

  describe('paginatedRequest', () => {
    it('should return all', done => {
      const requestStub = sandbox.stub();
      const callbackSpy = sandbox.spy();

      requestStub.onCall(0).callsArgOnWithAsync(1, null, null, {
        LastEvaluatedKey: 1,
        ConsumedCapacity: { CapacityUnits: 100 },
        Items: [1, 2]
      });
      requestStub.onCall(1).callsArgOnWithAsync(1, null, null, {
        LastEvaluatedKey: null,
        ConsumedCapacity: { CapacityUnits: 30 },
        Items: [3]
      });

      utils.paginatedRequest(
        {
          options: {
            loadAll: true,
            consumeThroughputLimit: 50
          },
          table: {
            tableName: () => 'testTable',
          },
          buildRequest: () => null,
          startKey: () => null
        },
        requestStub,
        callbackSpy
      );
      // async use process.nextTick cannot be mocked
      setImmediate(() => {
        expect(requestStub.callCount).to.be.equal(1);
        clock.tick(2000);
        setImmediate(() => {
          expect(requestStub.callCount).to.be.equal(2);
          expect(callbackSpy.callCount).to.equal(0);
          setImmediate(() => {
            expect(callbackSpy.callCount).to.equal(1);
            expect(callbackSpy.getCall(0).args[1]).to.deep.equal({
              Count: 0,
              ConsumedCapacity: { CapacityUnits: 130, TableName: 'testTable' },
              Items: [1, 2, 3],
            });
          });
          done();
        });
      });
    });
  });

  describe('streamRequest', () => {
    it('should read all', done => {
      const requestStub = sandbox.stub();
      requestStub.onCall(0).callsArgOnWithAsync(1, null, null, {
        LastEvaluatedKey: 1,
        ConsumedCapacity: { CapacityUnits: 100 },
        Items: [1, 2]
      });
      requestStub.onCall(1).callsArgOnWithAsync(1, null, null, {
        LastEvaluatedKey: 2,
        ConsumedCapacity: { CapacityUnits: 100 },
        Items: [3, 4]
      });
      requestStub.onCall(2).callsArgOnWithAsync(1, null, null, {
        LastEvaluatedKey: null,
        ConsumedCapacity: { CapacityUnits: 30 },
        Items: [5]
      });
      const stream = utils.streamRequest({
        options: {
          loadAll: true,
          consumeThroughputLimit: 50
        },
        buildRequest: () => null,
        startKey: () => null
      }, requestStub);

      let batch = 0;
      setInterval(() => clock.tick(500), 10);
      stream.on('data', () => {
        expect(requestStub.callCount).to.equal(++batch);
        if (batch === 3) {
          done();
        }
      });
    });
  });
});
