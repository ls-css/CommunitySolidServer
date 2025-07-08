import type { Readable } from 'node:stream';
import { Client } from 'minio';
import type { Representation } from '../../http/representation/Representation';
import { RepresentationMetadata } from '../../http/representation/RepresentationMetadata';
import type { ResourceIdentifier } from '../../http/representation/ResourceIdentifier';
import { getLoggerFor } from '../../logging/LogUtil';
import { NotFoundHttpError } from '../../util/errors/NotFoundHttpError';
import { UnsupportedMediaTypeHttpError } from '../../util/errors/UnsupportedMediaTypeHttpError';
import { guardStream } from '../../util/GuardedStream';
import type { Guarded } from '../../util/GuardedStream';
import { serializeQuads, parseQuads } from '../../util/QuadUtil';
import { isContainerIdentifier } from '../../util/PathUtil';
import type { IdentifierStrategy } from '../../util/identifiers/IdentifierStrategy';
import type { DataAccessor } from './DataAccessor';

export interface MinioDataAccessorOptions {
  endPoint: string;
  port: number;
  useSSL: boolean;
  accessKey: string;
  secretKey: string;
  bucket: string;
  identifierStrategy: IdentifierStrategy;
}

/**
 * DataAccessor that stores data on a MinIO/S3 backend.
 */
export class MinioDataAccessor implements DataAccessor {
  protected readonly logger = getLoggerFor(this);
  private readonly client: Client;
  private readonly bucket: string;
  private readonly identifierStrategy: IdentifierStrategy;

  public constructor(options: MinioDataAccessorOptions) {
    this.client = new Client({
      endPoint: options.endPoint,
      port: options.port,
      useSSL: options.useSSL,
      accessKey: options.accessKey,
      secretKey: options.secretKey,
    });
    this.bucket = options.bucket;
    this.identifierStrategy = options.identifierStrategy;
  }

  private keyFor(identifier: ResourceIdentifier, isMeta = false): string {
    let key = identifier.path.startsWith('/') ? identifier.path.slice(1) : identifier.path;
    if (isMeta) {
      key += '.meta';
    }
    return key;
  }

  public async canHandle(representation: Representation): Promise<void> {
    if (!representation.binary) {
      throw new UnsupportedMediaTypeHttpError('Only binary data is supported.');
    }
  }

  public async getData(identifier: ResourceIdentifier): Promise<Guarded<Readable>> {
    try {
      const stream: Readable = await new Promise((resolve, reject): void => {
        this.client.getObject(this.bucket, this.keyFor(identifier), (err, data) => {
          if (err) {
            reject(err);
          } else {
            resolve(data);
          }
        });
      });
      return guardStream(stream);
    } catch (err: any) {
      this.logger.warn(`Data not found for ${identifier.path}`);
      throw new NotFoundHttpError();
    }
  }

  public async getMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
    try {
      const stream: Readable = await new Promise((resolve, reject): void => {
        this.client.getObject(this.bucket, this.keyFor(identifier, true), (err, data) => {
          if (err) {
            reject(err);
          } else {
            resolve(data);
          }
        });
      });
      const quads = await parseQuads(guardStream(stream), { format: 'text/turtle', baseIRI: identifier.path });
      return new RepresentationMetadata(identifier).addQuads(quads);
    } catch (err: any) {
      this.logger.warn(`Metadata not found for ${identifier.path}`);
      throw new NotFoundHttpError();
    }
  }

  public async* getChildren(identifier: ResourceIdentifier): AsyncIterableIterator<RepresentationMetadata> {
    const prefix = this.keyFor(identifier).replace(/\/?$/, '/');
    const stream = this.client.listObjectsV2(this.bucket, prefix, false);
    const objects: string[] = [];
    await new Promise((resolve, reject): void => {
      stream.on('data', (obj) => {
        if (!obj.name.endsWith('.meta')) {
          objects.push(obj.name);
        }
      });
      stream.on('end', resolve);
      stream.on('error', reject);
    });
    for (const name of objects) {
      const id = { path: '/' + name };
      yield new RepresentationMetadata(id);
    }
  }

  public async writeDocument(identifier: ResourceIdentifier, data: Guarded<Readable>, metadata: RepresentationMetadata): Promise<void> {
    const meta = serializeQuads(metadata.quads(), 'text/turtle');
    await new Promise((resolve, reject): void => {
      this.client.putObject(this.bucket, this.keyFor(identifier, true), meta, (err) => err ? reject(err) : resolve(undefined));
    });
    await new Promise((resolve, reject): void => {
      this.client.putObject(this.bucket, this.keyFor(identifier), data, (err) => err ? reject(err) : resolve(undefined));
    });
  }

  public async writeContainer(identifier: ResourceIdentifier, metadata: RepresentationMetadata): Promise<void> {
    const meta = serializeQuads(metadata.quads(), 'text/turtle');
    await new Promise((resolve, reject): void => {
      this.client.putObject(this.bucket, this.keyFor(identifier, true), meta, (err) => err ? reject(err) : resolve(undefined));
    });
    await new Promise((resolve, reject): void => {
      this.client.putObject(this.bucket, this.keyFor(identifier), Buffer.alloc(0), (err) => err ? reject(err) : resolve(undefined));
    });
  }

  public async writeMetadata(identifier: ResourceIdentifier, metadata: RepresentationMetadata): Promise<void> {
    const meta = serializeQuads(metadata.quads(), 'text/turtle');
    await new Promise((resolve, reject): void => {
      this.client.putObject(this.bucket, this.keyFor(identifier, true), meta, (err) => err ? reject(err) : resolve(undefined));
    });
  }

  public async deleteResource(identifier: ResourceIdentifier): Promise<void> {
    await new Promise((resolve) => {
      this.client.removeObject(this.bucket, this.keyFor(identifier, true), () => resolve(undefined));
    });
    if (isContainerIdentifier(identifier)) {
      const prefix = this.keyFor(identifier).replace(/\/?$/, '/');
      const stream = this.client.listObjectsV2(this.bucket, prefix, true);
      const names: string[] = [];
      await new Promise((resolve, reject): void => {
        stream.on('data', (obj) => names.push(obj.name));
        stream.on('end', resolve);
        stream.on('error', reject);
      });
      if (names.length > 0) {
        await new Promise((resolve, reject): void => {
          this.client.removeObjects(this.bucket, names, (err) => err ? reject(err) : resolve(undefined));
        });
      }
    } else {
      await new Promise((resolve) => {
        this.client.removeObject(this.bucket, this.keyFor(identifier), () => resolve(undefined));
      });
    }
  }
}
