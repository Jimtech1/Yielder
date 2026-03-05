"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.logger = exports.PinoLoggerService = void 0;
const pino_1 = __importDefault(require("pino"));
class PinoLoggerService {
    constructor() {
        this.logger = (0, pino_1.default)({
            transport: {
                targets: [
                    {
                        target: 'pino/file',
                        options: { destination: './logs/error.log' },
                        level: 'error',
                    },
                    {
                        target: 'pino-pretty',
                        options: { colorize: true },
                        level: 'info',
                    },
                ],
            },
        });
    }
    log(message, context) {
        this.logger.info({ context }, message);
    }
    error(message, trace, context) {
        this.logger.error({ trace, context }, message);
    }
    warn(message, context) {
        this.logger.warn({ context }, message);
    }
    debug(message, context) {
        this.logger.debug({ context }, message);
    }
    verbose(message, context) {
        this.logger.trace({ context }, message);
    }
}
exports.PinoLoggerService = PinoLoggerService;
exports.logger = new PinoLoggerService();
