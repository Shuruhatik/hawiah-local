import * as fs from 'fs/promises';
import * as path from 'path';
import * as yaml from 'js-yaml';
import { Writer } from 'writerx';
import { IDriver, Query, Data } from '../interfaces/IDriver';

/**
 * Driver implementation for YAML files.
 * Stores data in a human-readable YAML format.
 */
export class YAMLDriver implements IDriver {
    private filePath: string;
    private writer: Writer;
    private yamlOptions: yaml.DumpOptions;
    private data: Data[];
    private isConnected: boolean = false;

    /**
     * Creates a new instance of YAMLDriver
     * @param filePath - Path to the YAML file
     * @param yamlOptions - YAML dump options
     */
    constructor(filePath: string, yamlOptions?: yaml.DumpOptions) {
        this.filePath = path.resolve(filePath);
        this.writer = new Writer(this.filePath);
        this.yamlOptions = yamlOptions || {
            indent: 2,
            lineWidth: 120,
            noRefs: true,
            sortKeys: false,
        };
        this.data = [];
    }

    /**
     * Connects to the YAML file.
     * Loads existing data or creates a new file.
     */
    async connect(): Promise<void> {
        const dir = path.dirname(this.filePath);
        await fs.mkdir(dir, { recursive: true });

        try {
            const content = await fs.readFile(this.filePath, 'utf8');
            try {
                const parsed = yaml.load(content);
                this.data = Array.isArray(parsed) ? parsed : [];
            } catch (error) {
                this.data = [];
            }
        } catch (error: any) {
            if (error.code === 'ENOENT') {
                this.data = [];
                await this.saveToFile();
            } else {
                throw error;
            }
        }

        this.isConnected = true;
    }

    /**
     * Disconnects from the YAML file.
     */
    async disconnect(): Promise<void> {
        this.isConnected = false;
    }

    /**
     * Saves the data to the YAML file.
     * @private
     */
    private async saveToFile(): Promise<void> {
        const yamlStr = yaml.dump(this.data, this.yamlOptions);
        await this.writer.write(yamlStr);
    }

    /**
     * Inserts a new record.
     * @param data - The data to insert
     * @returns The inserted record with ID
     */
    async set(data: Data): Promise<Data> {
        this.ensureConnected();

        const id = this.generateId();
        const record = {
            ...data,
            _id: id,
            _createdAt: new Date().toISOString(),
            _updatedAt: new Date().toISOString(),
        };

        this.data.push(record);

        return record;
    }

    /**
     * Retrieves records matching the query.
     * @param query - The query criteria
     * @returns Array of matching records
     */
    async get(query: Query): Promise<Data[]> {
        this.ensureConnected();

        if (Object.keys(query).length === 0) {
            return [...this.data];
        }

        return this.data.filter(record => this.matchesQuery(record, query));
    }

    /**
     * Retrieves a single record matching the query.
     * @param query - The query criteria
     * @returns The first matching record or null
     */
    async getOne(query: Query): Promise<Data | null> {
        this.ensureConnected();

        const results = await this.get(query);
        return results.length > 0 ? results[0] : null;
    }

    /**
     * Updates records matching the query.
     * @param query - The query criteria
     * @param data - The data to update
     * @returns The number of updated records
     */
    async update(query: Query, data: Data): Promise<number> {
        this.ensureConnected();

        let count = 0;
        for (let i = 0; i < this.data.length; i++) {
            if (this.matchesQuery(this.data[i], query)) {
                this.data[i] = {
                    ...this.data[i],
                    ...data,
                    _id: this.data[i]._id,
                    _createdAt: this.data[i]._createdAt,
                    _updatedAt: new Date().toISOString(),
                };
                count++;
            }
        }

        return count;
    }

    /**
     * Deletes records matching the query.
     * @param query - The query criteria
     * @returns The number of deleted records
     */
    async delete(query: Query): Promise<number> {
        this.ensureConnected();

        const beforeLength = this.data.length;
        this.data = this.data.filter(record => !this.matchesQuery(record, query));
        const count = beforeLength - this.data.length;

        return count;
    }

    /**
     * Checks if any record matches the query.
     * @param query - The query criteria
     * @returns True if a match exists, false otherwise
     */
    async exists(query: Query): Promise<boolean> {
        this.ensureConnected();

        return this.data.some(record => this.matchesQuery(record, query));
    }

    /**
     * Counts records matching the query.
     * @param query - The query criteria
     * @returns The number of matching records
     */
    async count(query: Query): Promise<number> {
        this.ensureConnected();

        if (Object.keys(query).length === 0) {
            return this.data.length;
        }

        return this.data.filter(record => this.matchesQuery(record, query)).length;
    }

    /**
     * Ensures the driver is connected before executing operations.
     * @throws Error if driver is not connected
     * @private
     */
    private ensureConnected(): void {
        if (!this.isConnected) {
            throw new Error('Driver not connected. Call connect() first.');
        }
    }

    /**
     * Generates a unique ID for records.
     * @returns A unique string ID
     * @private
     */
    private generateId(): string {
        return `${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
    }

    /**
     * Checks if a record matches the query criteria.
     * @param record - The record to check
     * @param query - The query criteria
     * @returns True if the record matches
     * @private
     */
    private matchesQuery(record: Data, query: Query): boolean {
        for (const [key, value] of Object.entries(query)) {
            if (JSON.stringify(record[key]) !== JSON.stringify(value)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Gets all data in memory.
     * @returns All records
     */
    getAllData(): Data[] {
        return [...this.data];
    }

    /**
     * Manually saves data to file.
     * Useful when autoSave is disabled.
     */
    async save(): Promise<void> {
        this.ensureConnected();
        await this.saveToFile();
    }

    /**
     * Clears all data from memory and file.
     */
    async clear(): Promise<void> {
        this.ensureConnected();
        this.data = [];
    }

    /**
     * Deletes the YAML file.
     */
    async drop(): Promise<void> {
        this.ensureConnected();
        if (await this.fileExists()) {
            await fs.unlink(this.filePath);
        }
        this.data = [];
    }

    /**
     * Reloads data from the YAML file.
     * Useful for syncing with external changes.
     */
    async reload(): Promise<void> {
        this.ensureConnected();

        if (await this.fileExists()) {
            const content = await fs.readFile(this.filePath, 'utf8');
            try {
                const parsed = yaml.load(content);
                this.data = Array.isArray(parsed) ? parsed : [];
            } catch (error) {
                throw new Error(`Failed to parse YAML file: ${error}`);
            }
        }
    }

    /**
     * Gets the YAML file path.
     * @returns The file path
     */
    getFilePath(): string {
        return this.filePath;
    }

    private async fileExists(): Promise<boolean> {
        try {
            await fs.access(this.filePath);
            return true;
        } catch {
            return false;
        }
    }
}
