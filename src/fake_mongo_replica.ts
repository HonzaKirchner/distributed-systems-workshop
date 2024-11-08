import type { FakeMongoPrimary } from './fake_mongo_primary';
import type { Document, IFakeMongoReplica } from './types';

/**
 * TODO: Implement a replication algorithm that synchronizes data from the primary.
 * See README.md for more details.
 * Run `npm test:replication` to test your implementation.
 */
export class FakeMongoReplica implements IFakeMongoReplica {
    private primaryMongo: FakeMongoPrimary
    private data = new Map<string, Map<string, Document>>();

    constructor(fakeMongoPrimary: FakeMongoPrimary) {
        this.primaryMongo = fakeMongoPrimary
        fakeMongoPrimary.subscribeToChanges(this)
    }

    public async get(collection: string, documentId: string) {
        if (this.data.has(collection)) {
            return this.data.get(collection)!.get(documentId)
        }
        console.error('Collection does not exist');
        return
    }

    public async upsert(collection: string, document: Document) {
        await this.primaryMongo.upsert(collection, document)
    }

    public async delete(collection: string, documentId: string) {
        await this.primaryMongo.delete(collection, documentId)
    }

    public handlePrimaryChanges(changes: { operation: 'UPSERT' | 'DELETE', collection: string, documentId: string, document?: Document }[]) {
        changes.forEach(async change => {
            if (change.operation === 'UPSERT') {
                if (change.document) {
                    if (!this.data.has(change.collection)) {
                        this.data.set(change.collection, new Map());
                    }

                    this.data.get(change.collection)!.set(change.document._id, change.document);
                }
                return
            }

            if (this.data.has(change.collection)) {
                this.data.get(change.collection)!.delete(change.documentId);
            }
        })
    }
}