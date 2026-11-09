import { ChannelWrapper } from '../../shared/rabbitmq/RabbitMQClient.js';
import { QUEUES } from '../../shared/rabbitmq/ExchangeTypes.js';
import { NotificationBroadcastEvent } from '../../shared/events/NotificationEvents.js';

export class NotificationService {
  private sentEmails: NotificationBroadcastEvent[] = [];
  private sentSms: NotificationBroadcastEvent[] = [];
  private sentSlackAlerts: NotificationBroadcastEvent[] = [];

  constructor(private channel: ChannelWrapper) {}

  async start(): Promise<void> {
    console.log('[NotificationService] Starting Fanout Notification Subscribers...');

    await this.channel.consume(QUEUES.NOTIFICATIONS_EMAIL, (msg) => {
      if (!msg) return;
      const notif: NotificationBroadcastEvent = JSON.parse(msg.content.toString());
      this.sentEmails.push(notif);
      console.log(`[Notification Service -> EMAIL] Sent email to ${notif.recipientEmail || 'customer'}: "${notif.title}"`);
      this.channel.ack(msg);
    });

    await this.channel.consume(QUEUES.NOTIFICATIONS_SMS, (msg) => {
      if (!msg) return;
      const notif: NotificationBroadcastEvent = JSON.parse(msg.content.toString());
      this.sentSms.push(notif);
      console.log(`[Notification Service -> SMS] Sent SMS to ${notif.recipientPhone || '+1-555-0199'}: "${notif.title}"`);
      this.channel.ack(msg);
    });

    await this.channel.consume(QUEUES.NOTIFICATIONS_SLACK, (msg) => {
      if (!msg) return;
      const notif: NotificationBroadcastEvent = JSON.parse(msg.content.toString());
      this.sentSlackAlerts.push(notif);
      console.log(`[Notification Service -> SLACK] Posted to #ops-alerts: "[${notif.type}] ${notif.title}"`);
      this.channel.ack(msg);
    });
  }

  getDeliveredCounts(): { emails: number; sms: number; slack: number } {
    return {
      emails: this.sentEmails.length,
      sms: this.sentSms.length,
      slack: this.sentSlackAlerts.length,
    };
  }
}
