package win.ixuni.chimera.test.s3.crossdriver;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 驱动组合池
 * <p>
 * Generates all possible cross-driver combinations and supports random sampling
 */
public class DriverCombinationPool {

    private final List<DriverConfig> drivers;
    private final List<DriverCombination> allCombinations;
    private final Random random;

    public DriverCombinationPool(List<DriverConfig> drivers) {
        this.drivers = drivers.stream()
                .filter(DriverConfig::isEnabled)
                .collect(Collectors.toList());
        this.allCombinations = generateAllCombinations();
        this.random = new Random();
    }

    /**
     * Generate all possible N*(N-1) combinations
     * Each driver can be source or target, but source and target cannot be the same driver
     */
    private List<DriverCombination> generateAllCombinations() {
        List<DriverCombination> combinations = new ArrayList<>();

        for (DriverConfig source : drivers) {
            for (DriverConfig target : drivers) {
                // Skip the same driver instance
                if (source.getName().equals(target.getName())) {
                    continue;
                }

                combinations.add(DriverCombination.builder()
                        .source(source)
                        .target(target)
                        .build());
            }
        }

        return combinations;
    }

    /**
     * Get all combinations
     */
    public List<DriverCombination> getAllCombinations() {
        return Collections.unmodifiableList(allCombinations);
    }

    /**
     * 随机抽取 N 个组合
     * 
     * @param count sample count; if <= 0, returns all combinations
     * @return 随机选择的组合列表
     */
    public List<DriverCombination> pickRandom(int count) {
        if (count <= 0 || count >= allCombinations.size()) {
            return new ArrayList<>(allCombinations);
        }

        List<DriverCombination> shuffled = new ArrayList<>(allCombinations);
        Collections.shuffle(shuffled, random);
        return shuffled.subList(0, count);
    }

    /**
     * 只获取跨驱动类型的组合（源和目标类型不同）
     */
    public List<DriverCombination> getCrossTypeCombinations() {
        return allCombinations.stream()
                .filter(DriverCombination::isCrossDriver)
                .collect(Collectors.toList());
    }

    /**
     * 随机抽取 N 个跨类型组合
     */
    public List<DriverCombination> pickRandomCrossType(int count) {
        List<DriverCombination> crossType = getCrossTypeCombinations();

        if (count <= 0 || count >= crossType.size()) {
            return new ArrayList<>(crossType);
        }

        List<DriverCombination> shuffled = new ArrayList<>(crossType);
        Collections.shuffle(shuffled, random);
        return shuffled.subList(0, count);
    }

    /**
     * 获取驱动数量
     */
    public int getDriverCount() {
        return drivers.size();
    }

    /**
     * 获取总组合数量
     */
    public int getTotalCombinationCount() {
        return allCombinations.size();
    }

    /**
     * 获取跨类型组合数量
     */
    public int getCrossTypeCombinationCount() {
        return (int) allCombinations.stream()
                .filter(DriverCombination::isCrossDriver)
                .count();
    }

    /**
     * 转为 JUnit 5 ParameterizedTest 的参数流
     */
    public Stream<DriverCombination> stream() {
        return allCombinations.stream();
    }

    @Override
    public String toString() {
        return String.format("DriverCombinationPool{drivers=%d, combinations=%d, crossType=%d}",
                getDriverCount(), getTotalCombinationCount(), getCrossTypeCombinationCount());
    }
}
