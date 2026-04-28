
def generate_response(query, model, client, context):
    """
    Generate a grounded RAG answer using retrieved document context.
    """

    system_prompt = """
    You are an AI assistant that answers questions based on provided document context.

    Your rules:
    - Answer the user's question in 1–2 clear sentences.
    - Use ONLY the provided document context.
    - Do NOT use bullet points, lists, or headings.
    - Do NOT add follow-up actions unless explicitly asked.
    - Be precise, factual, and direct.
    - If the answer cannot be determined from the context, say so plainly.
    """

    user_prompt = f"""
    QUESTION:
    {query}

    DOCUMENT CONTEXT:
    {context}

    INSTRUCTIONS:
    Respond in 1–2 sentences that directly answer the question.
    """

    # For OpenAI:
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        max_tokens=500,
        temperature=0.2
    )

    return response.choices[0].message.content

