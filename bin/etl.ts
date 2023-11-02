#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { EtlStack } from '../lib/etl-stack';

const app = new cdk.App();
new EtlStack(app, 'EtlStack');
