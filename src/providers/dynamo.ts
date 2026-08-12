/* =============================================================================
 *  DynamoDB plumbing shared by every aws-mode component.
 * =============================================================================
 *  Two jobs, both boring, both previously copy-pasted into four places:
 *
 *  1. LAZY CLIENTS. The @aws-sdk packages are optionalDependencies — local mode
 *     must never load them. Every client is therefore require()d on first use
 *     and memoized per configuration.
 *
 *  2. PAGINATION. A DynamoDB Query returns AT MOST 1MB and then stops, handing
 *     back a LastEvaluatedKey. Reading `Items` and ignoring that key does not
 *     error — it silently returns a PREFIX of the answer. For a fan-out lookup
 *     that means "everyone past the first megabyte stops receiving messages",
 *     with no failure anywhere to point at. queryAll/queryPage exist so that
 *     mistake can't be made by omission.
 * ========================================================================== */

import type { Page, PageCursor } from '../ports';

export type { Page, PageCursor };

const clients = new Map<string, any>();

/**
 * Memoized DynamoDBDocumentClient. `key` separates configurations (e.g. the
 * registry needs removeUndefinedValues for GraphQL variables, the connection
 * store does not).
 */
export function docClient(key = 'default', options: Record<string, unknown> = {}): any {
  let client = clients.get(key);
  if (!client) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
    client = DynamoDBDocumentClient.from(new DynamoDBClient({}), options);
    clients.set(key, client);
  }
  return client;
}

/** One page of a Query, plus the cursor to continue from. */
export async function queryPage(
  doc: any,
  params: Record<string, unknown>,
  cursor?: PageCursor,
): Promise<Page<any>> {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
  const out = await doc.send(
    new QueryCommand({ ...params, ...(cursor ? { ExclusiveStartKey: cursor } : {}) }),
  );
  return { items: out.Items ?? [], cursor: out.LastEvaluatedKey };
}

/** Every page of a Query, concatenated. Use only where the result set is known
 *  to be small; fan-out paths should page explicitly so one huge topic can't be
 *  loaded into a single Lambda's memory. */
export async function queryAll(doc: any, params: Record<string, unknown>): Promise<any[]> {
  const items: any[] = [];
  let cursor: PageCursor | undefined;
  do {
    const page = await queryPage(doc, params, cursor);
    items.push(...page.items);
    cursor = page.cursor;
  } while (cursor);
  return items;
}
