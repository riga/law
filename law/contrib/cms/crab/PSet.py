# coding: utf-8

"""
Minimal valid configuration.
"""

import FWCore.ParameterSet.Config as cms


process = cms.Process("LAW")

process.source = cms.Source(
    "PoolSource",
    fileNames=cms.untracked.vstring([""]),
)

process.output = cms.OutputModule(
    "PoolOutputModule",
    fileName=cms.untracked.string("out.root"),
)

process.maxEvents = cms.untracked.PSet(
    input=cms.untracked.int32(1),
)

process.options = cms.untracked.PSet(
    allowUnscheduled=cms.untracked.bool(True),
    wantSummary=cms.untracked.bool(False),
)

process.out = cms.EndPath(process.output)
