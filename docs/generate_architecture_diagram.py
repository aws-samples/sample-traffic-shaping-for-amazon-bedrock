"""
Bedrock Traffic Shaper -- Architecture Diagram Generator

Professional AWS architecture diagram with dual-flow rate limiter.
Simplified edge set to minimize crossings. LR layout.
"""

import os

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.integration import StepFunctions, Eventbridge
from diagrams.aws.network import APIGateway
from diagrams.aws.database import Dynamodb
from diagrams.aws.ml import Bedrock
from diagrams.aws.general import User

graph_attr = {
    "fontsize": "22",
    "fontname": "Helvetica Bold",
    "bgcolor": "white",
    "pad": "0.8",
    "nodesep": "1.0",
    "ranksep": "1.6",
    "splines": "spline",
    "dpi": "200",
}

node_attr = {
    "fontsize": "11",
    "fontname": "Helvetica",
    "fontcolor": "#1a1a1a",
}

edge_attr = {
    "fontsize": "9",
    "fontname": "Helvetica",
    "fontcolor": "#444444",
}

F1 = {"color": "#15803d", "style": "bold", "penwidth": "2.2"}
F2 = {"color": "#d97706", "style": "dashed", "penwidth": "2.2"}
DATA = {"color": "#64748b", "style": "dotted", "penwidth": "1.4"}
RESP = {"color": "#9ca3af", "style": "solid", "penwidth": "1.0"}

OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "architecture-diagram")

with Diagram(
    "Bedrock Traffic Shaper \u2014 Distributed Rate Limiter with TPM Tracking",
    filename=OUTPUT,
    outformat="png",
    direction="LR",
    show=False,
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):

    # ---- Left: Client ----
    client = User("Client")

    # ---- Ingress ----
    with Cluster(
        "Ingress & Orchestration",
        graph_attr={
            "bgcolor": "#f8fafc",
            "style": "rounded",
            "pencolor": "#cbd5e1",
            "penwidth": "1",
            "fontsize": "13",
            "fontname": "Helvetica Bold",
            "fontcolor": "#64748b",
        },
    ):
        apigw = APIGateway("API Gateway")
        sfn = StepFunctions("Step Functions\n(Task Token)")

    # ---- Processing ----
    with Cluster(
        "Lambda Functions",
        graph_attr={
            "bgcolor": "#eff6ff",
            "style": "rounded",
            "pencolor": "#3b82f6",
            "penwidth": "2",
            "fontsize": "13",
            "fontname": "Helvetica Bold",
            "fontcolor": "#1e3a5f",
        },
    ):
        budget_mgr = Lambda("Budget Manager\nGates on min(RPM, TPM)")
        bedrock_proc = Lambda("Bedrock Processor\nConverse API")
        queue_proc = Lambda("Queue Processor\nBatch Dequeue")

    # ---- State ----
    with Cluster(
        "State Management",
        graph_attr={
            "bgcolor": "#fefce8",
            "style": "rounded",
            "pencolor": "#ca8a04",
            "penwidth": "2",
            "fontsize": "13",
            "fontname": "Helvetica Bold",
            "fontcolor": "#854d0e",
        },
    ):
        dynamodb = Dynamodb("DynamoDB\nSingle Table Design")
        eventbridge = Eventbridge("EventBridge\nSchedule")

    # ---- Right: Bedrock ----
    bedrock = Bedrock("Amazon Bedrock\nClaude Opus | Jamba")

    # ==================================================================
    # FLOW 1: Immediate path -- green solid (primary L->R flow)
    # This is the main "spine" of the diagram
    # ==================================================================
    client >> Edge(label="Request", **F1) >> apigw
    apigw >> Edge(label="Invoke", **F1) >> sfn
    sfn >> Edge(label="Check\nCapacity", **F1) >> budget_mgr
    budget_mgr >> Edge(label="Capacity OK", **F1) >> bedrock_proc
    bedrock_proc >> Edge(label="Converse API", **F1) >> bedrock

    # Response (thin, same direction -- Bedrock back to SFN via callback)
    bedrock_proc >> Edge(label="Callback", **RESP) >> sfn

    # ==================================================================
    # FLOW 2: Queued path -- orange dashed
    # ==================================================================
    budget_mgr >> Edge(label="Enqueue\n(RPM/TPM Exceeded)", **F2) >> dynamodb
    eventbridge >> Edge(label="Trigger", **F2) >> queue_proc
    queue_proc >> Edge(label="Capacity\nAvailable", **F2) >> bedrock_proc

    # ==================================================================
    # Data plane -- dotted (DynamoDB central state)
    # Only show unique connections not already covered by Flow 2
    # ==================================================================
    queue_proc >> Edge(label="Dequeue", **DATA) >> dynamodb
    bedrock_proc >> Edge(label="Update\nTokens", **DATA) >> dynamodb
    dynamodb >> Edge(label="Queue\nNot Empty", **DATA) >> eventbridge
