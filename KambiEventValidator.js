const axios = require('axios');

const DEFAULT_KAMBI_BASE_URL =
    'https://odds.mrgreen.com/offering/v2018/mg/betoffer/event/';
const DEFAULT_MAXIMUM_PARALLEL_REQUESTS = 8;

class Worker {
    constructor(kambiBaseUrl) {
        this.kambiBaseUrl = kambiBaseUrl;
        this.isBusy = false;
        this.currentPromise = Promise.resolve();
    }

    async checkEventId(eventId) {
        if (this.isBusy) {
            throw new Error('worker is busy');
        }
        this.currentPromise = this.makeRequest(eventId);
        return await this.currentPromise;
    }

    async makeRequest(eventId) {
        this.isBusy = true;
        try {
            await axios({
                method: 'get',
                url: this.kambiBaseUrl + eventId,
                responseType: 'arraybuffer' // don't need to parse the response
            });
            return true;
        } catch (e) {
            console.log(`invalid id: ${eventId}`);
            return false;
        } finally {
            this.isBusy = false;
        }
    }
}

module.exports = class KambiEventValidator {
    constructor(kambiBaseUrl, maximumParallelRequests) {
        this.kambiBaseUrl = kambiBaseUrl || KAMBI_BASE_URL;
        this.maximumParallelRequests =
            maximumParallelRequests || DEFAULT_MAXIMUM_PARALLEL_REQUESTS;
        this.allValidEventIds = new Set();
        this.allInvalidEventIds = new Set();
        this.workers = [];
        for (let i = 0; i < this.maximumParallelRequests; i++) {
            this.workers.push(new Worker(this.kambiBaseUrl));
        }
        this.verificationPromises = [];
    }

    async getNextWorker() {
        // trying to find a non-busy worker
        let nonBusyWorker = this.workers.find(worker => !worker.isBusy);
        if (nonBusyWorker == null) {
            // if all workers are busy we wait for one
            await Promise.race(
                this.workers.map(worker => worker.currentPromise)
            );
            nonBusyWorker = this.workers.find(worker => !worker.isBusy);
        }
        return nonBusyWorker;
    }

    async getAllValidEventIds() {
        await Promise.all(this.verificationPromises);
        return Array.from(this.allValidEventIds);
    }

    async getAllInvalidEventIds() {
        await Promise.all(this.verificationPromises);
        return Array.from(this.allInvalidEventIds);
    }

    checkEventIdAgainstCache(eventId) {
        if (this.allValidEventIds.has(eventId)) {
            return 'valid';
        } else if (this.allInvalidEventIds.has(eventId)) {
            return 'invalid';
        }
        return 'unkown';
    }

    async verify(eventIds, maximumEventsRequired) {
        const doVerify = async() => {
            const validEventIds = new Set();
            const invalidEventIds = new Set();
            for (let i = 0; i < eventIds.length; i++) {
                if (validEventIds.length >= maximumEventsRequired) {
                    break;
                }
                switch (this.checkEventIdAgainstCache(eventId)) {
                    case 'valid':
                        validEventIds.add(eventId);
                        break;
                    case 'invalid':
                        invalidEventIds.add(eventId);
                        break;
                    case 'unkown':
                    default:
                        {
                            const worker = await this.getNextWorker;
                            // while waiting for a worker, another worker might have checked that eventId already
                            switch (this.checkEventIdAgainstCache(eventId)) {
                                case 'valid':
                                    validEventIds.add(eventId);
                                    break;
                                case 'invalid':
                                    invalidEventIds.add(eventId);
                                    break;
                                case 'unkown':
                                default:
                                    {
                                        const isEventIdValid = await worker.checkEventId(
                                            eventId
                                        );
                                        if (isEventIdValid) {
                                            validEventId.add(eventId);
                                            this.allValidEventIds.add(eventId);
                                        } else {
                                            invalidEventIds.add(eventId);
                                            this.allInvalidEventIds.add(
                                                eventId
                                            );
                                        }
                                    }
                                    break;
                            }
                        }
                        break;
                }
            }

            return {
                validEventIds: Array.from(validEventIds),
                invalidEventIds: Array.from(invalidEventIds)
            };
        };

        const verificationPromise = doVerify();
        this.verificationPromises.push(verificationPromise);
        return await verificationPromise;
    }
};
