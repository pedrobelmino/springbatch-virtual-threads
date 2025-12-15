package br.com.pedrobelmino.sbatch.virtualthreads.domain;

import java.math.BigDecimal;

public record CustomerJson(Long customerId, String fullName, String contact, BigDecimal value, String processedAt) {}
