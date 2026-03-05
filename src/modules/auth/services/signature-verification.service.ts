import { Injectable, Logger } from '@nestjs/common';
import { Keypair, StrKey } from 'stellar-sdk';
import { verifyMessage } from 'ethers';
import { createHash } from 'crypto';

const STELLAR_SIGNED_MESSAGE_PREFIX = 'Stellar Signed Message:\n';

@Injectable()
export class SignatureVerificationService {
  private readonly logger = new Logger(SignatureVerificationService.name);

  private buildSep53Payload(message: string): Buffer {
    const prefixBytes = Buffer.from(STELLAR_SIGNED_MESSAGE_PREFIX, 'utf8');
    const messageBytes = Buffer.from(message, 'utf8');
    return createHash('sha256')
      .update(Buffer.concat([prefixBytes, messageBytes]))
      .digest();
  }

  private buildAlbedoPayload(publicKey: string, message: string): Buffer {
    const albedoMessage = message.startsWith(`${publicKey}:`)
      ? message
      : `${publicKey}:${message}`;

    return createHash('sha256')
      .update(Buffer.from(albedoMessage, 'utf8'))
      .digest();
  }

  verifyEd25519Signature(publicKey: string, message: string, signature: string): boolean {
    try {
      // 1. Detect Key Type
      if (publicKey.startsWith('0x')) {
        return this.verifyEvmSignature(publicKey, message, signature);
      }

      // 2. Stellar Verification
      if (!StrKey.isValidEd25519PublicKey(publicKey)) {
        this.logger.warn(`Invalid public key format (not Stellar or EVM): ${publicKey}`);
        return false;
      }

      const keypair = Keypair.fromPublicKey(publicKey);
      const messageBytes = Buffer.from(message, 'utf8');
      
      // Try Base64 first
      let signatureBytes = Buffer.from(signature, 'base64');

      // If length is not 64 bytes (Ed25519 standard), try Hex
      if (signatureBytes.length !== 64) {
        // this.logger.debug(`Signature not 64 bytes (Base64), trying Hex...`);
        signatureBytes = Buffer.from(signature, 'hex');
      }

      if (signatureBytes.length !== 64) {
        this.logger.warn(`Invalid signature length: ${signatureBytes.length} (expected 64). Msg: ${signature.substring(0, 10)}...`);
        return false;
      }

      this.logger.log(`Verifying Stellar Sig. PubKey: ${publicKey}`);
      this.logger.log(`Message (${messageBytes.length} bytes): ${JSON.stringify(message)}`);
      // this.logger.log(`Sig (${signatureBytes.length} bytes): ${signature}`);

      // Legacy flow: raw message bytes (used by direct SDK signing flows).
      const isRawMessageValid = keypair.verify(messageBytes, signatureBytes);
      if (isRawMessageValid) {
        return true;
      }

      // Freighter v5+ flow: SEP-53 hash with "Stellar Signed Message:\n" prefix.
      const sep53Payload = this.buildSep53Payload(message);
      const isSep53Valid = keypair.verify(sep53Payload, signatureBytes);
      if (isSep53Valid) {
        this.logger.log('Signature verified using SEP-53 payload');
        return true;
      }

      // Albedo flow: signature over sha256("<pubkey>:<message>").
      const albedoPayload = this.buildAlbedoPayload(publicKey, message);
      const isAlbedoValid = keypair.verify(albedoPayload, signatureBytes);
      if (isAlbedoValid) {
        this.logger.log('Signature verified using Albedo payload');
        return true;
      }

      this.logger.error(`Verification failed for message: "${message.substring(0, 20)}..."`);
      return false;
    } catch (error) {
        this.logger.error(`Signature verification failed: ${error.message}`);
        return false;
    }
  }

  private verifyEvmSignature(address: string, message: string, signature: string): boolean {
    try {
      // Verify signature retrieves the signer address
      const signerAddress = verifyMessage(message, signature);
      
      // Compare recovered address with provided address (case-insensitive)
      return signerAddress.toLowerCase() === address.toLowerCase();
    } catch (error) {
      this.logger.error(`EVM verification failed: ${error.message}`);
      return false;
    }
  }
}
