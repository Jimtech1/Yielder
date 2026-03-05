
import { Processor, WorkerHost } from '@nestjs/bullmq';

@Processor('indexer')
export class IndexerWorker extends WorkerHost {
  async process(job: any) {
    console.log('Indexing job', job.data);
  }
}
