import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  DynamoSubscriptionRegistry,
  GqlSubscriptionRecord,
  GqlSubscriptionRegistry,
  InMemorySubscriptionRegistry,
} from '../../../src/graphql';
import { fakeDynamo } from '../../helpers/fake-dynamo';

vi.mock('../../../src/providers/dynamo', async importOriginal => {
  const { fakeDynamo } = await import('../../helpers/fake-dynamo');
  return {
    ...(await importOriginal<typeof import('../../../src/providers/dynamo')>()),
    docClient: (_key?: string, options?: any) => fakeDynamo.client(options),
  };
});

beforeEach(() => fakeDynamo.reset());
afterEach(() => vi.unstubAllEnvs());

const sub = (connectionId: string, subscriptionId: string, topics: string[], over: Partial<GqlSubscriptionRecord> = {}) => ({
  connectionId, subscriptionId, topics,
  query: 'subscription { x }', variables: null, operationName: null,
  ...over,
});

describe.each<[string, () => GqlSubscriptionRegistry]>([
  ['InMemorySubscriptionRegistry', () => new InMemorySubscriptionRegistry()],
  ['DynamoSubscriptionRegistry', () => new DynamoSubscriptionRegistry()],
])('%s', (_name, make) => {
  it('finds every subscription awaiting a topic', async () => {
    const registry = make();
    await registry.add(sub('c1', 's1', ['A', 'B']));
    await registry.add(sub('c2', 's1', ['A']));
    await registry.add(sub('c2', 's2', ['C'], { variables: { feed: 'x' }, operationName: 'Op', connectionData: { user: 'ada' } }));
    expect((await registry.byTopic('A')).map(s => `${s.connectionId}/${s.subscriptionId}`).sort()).toEqual([
      'c1/s1', 'c2/s1',
    ]);
    expect(await registry.byTopic('C')).toEqual([
      sub('c2', 's2', ['C'], { variables: { feed: 'x' }, operationName: 'Op', connectionData: { user: 'ada' } }),
    ]);
    expect((await registry.pageByTopic!('B')).items).toHaveLength(1);
    expect(await registry.byTopic('none')).toEqual([]);
  });

  it('forgets one subscription on complete', async () => {
    const registry = make();
    await registry.add(sub('c1', 's1', ['A', 'B']));
    await registry.add(sub('c1', 's2', ['A']));
    await registry.remove('c1', 's1');
    expect((await registry.byTopic('A')).map(s => s.subscriptionId)).toEqual(['s2']);
    expect(await registry.byTopic('B')).toEqual([]);
    await registry.remove('c1', 'never-existed');
    await registry.remove('nobody', 's1');
  });

  it('forgets a connection\'s every subscription on disconnect', async () => {
    const registry = make();
    await registry.add(sub('c1', 's1', ['A']));
    await registry.add(sub('c1', 's2', ['B']));
    await registry.add(sub('c2', 's1', ['A']));
    await registry.removeAll('c1');
    expect((await registry.byTopic('A')).map(s => s.connectionId)).toEqual(['c2']);
    expect(await registry.byTopic('B')).toEqual([]);
    await registry.removeAll('nobody');
  });
});

describe('DynamoSubscriptionRegistry storage', () => {
  it('writes a forward row per topic and one reverse row', async () => {
    await new DynamoSubscriptionRegistry().add(sub('c1', 's1', ['A', 'B'], { variables: { maybe: undefined } as any }));
    expect(fakeDynamo.items('connections').map(i => `${i.pk} | ${i.sk}`)).toEqual([
      'CONN#c1 | GQLSUB#s1',
      'GQLTOPIC#A | CONN#c1#SUB#s1',
      'GQLTOPIC#B | CONN#c1#SUB#s1',
    ]);
  });

  it('reads topics with a consistent read, page by page', async () => {
    const registry = new DynamoSubscriptionRegistry();
    for (const id of ['s1', 's2', 's3']) await registry.add(sub('c1', id, ['A']));
    fakeDynamo.pageSize = 2;
    const first = await registry.pageByTopic('A');
    expect(first.items).toHaveLength(2);
    const second = await registry.pageByTopic('A', first.cursor);
    expect(second.items.map(s => s.subscriptionId)).toEqual(['s3']);
    expect(second.cursor).toBeUndefined();
    expect(await registry.byTopic('A')).toHaveLength(3);
    expect(fakeDynamo.calls.filter(c => c.command === 'QueryCommand').every(c => c.input.ConsistentRead)).toBe(true);
  });

  it('hydrates a row written without the optional fields', async () => {
    fakeDynamo.seed('connections', {
      pk: 'GQLTOPIC#A', sk: 'CONN#c#SUB#s', connectionId: 'c', subscriptionId: 's', query: 'q',
    });
    expect(await new DynamoSubscriptionRegistry().byTopic('A')).toEqual([
      { connectionId: 'c', subscriptionId: 's', topics: [], query: 'q', variables: null, operationName: null },
    ]);
  });

  it('cleans up the reverse row even when it lists no topics', async () => {
    fakeDynamo.seed('connections', { pk: 'CONN#c', sk: 'GQLSUB#s' });
    const registry = new DynamoSubscriptionRegistry();
    await registry.removeAll('c');
    expect(fakeDynamo.items('connections')).toEqual([]);
  });

  it('takes its table from CONNECTIONS_TABLE', async () => {
    vi.stubEnv('CONNECTIONS_TABLE', 'custom');
    await new DynamoSubscriptionRegistry().add(sub('c', 's', ['A']));
    expect(fakeDynamo.items('custom')).toHaveLength(2);
  });
});
