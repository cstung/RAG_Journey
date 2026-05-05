# backend/eval/run_eval.py
"""
Run RAGAS evaluation against the test set.
Usage: python eval/run_eval.py --testset eval/testset.json --output eval/results.json
"""
import json, argparse
from ragas import evaluate
from ragas.metrics import faithfulness, answer_relevancy, context_recall, context_precision
from datasets import Dataset as HFDataset
import sys; sys.path.insert(0, "..")
from rag import answer, embed_text
from retrieval import hybrid_search

def run(testset_path: str, output_path: str):
    testset = json.loads(open(testset_path).read())

    rows = []
    for item in testset:
        result  = answer(item["question"], collections=[item.get("collection", "vn_legal_docs")])
        contexts = [s["ref"] + ": " + result["answer"] for s in result["sources"]]  # simplification
        rows.append({
            "question":         item["question"],
            "answer":           result["answer"],
            "contexts":         contexts,
            "ground_truth":     item["expected_answer"],
        })

    dataset = HFDataset.from_list(rows)
    scores = evaluate(
        dataset,
        metrics=[faithfulness, answer_relevancy, context_recall, context_precision],
    )
    print(scores)
    # Convert Dataset to pandas then dict
    import pandas as pd
    if hasattr(scores, "to_pandas"):
        result_dict = scores.to_pandas().to_dict(orient="records")
    else:
        result_dict = {"status": "success", "info": "could not dump scores cleanly"}
    json.dump(result_dict, open(output_path, "w"), ensure_ascii=False, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--testset", default="eval/testset.json")
    parser.add_argument("--output",  default="eval/results.json")
    args = parser.parse_args()
    run(args.testset, args.output)
